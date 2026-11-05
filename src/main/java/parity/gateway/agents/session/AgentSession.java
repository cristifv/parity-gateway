package parity.gateway.agents.session;

import java.time.Instant;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.spi.LoggingEventBuilder;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.WebSocketSession;
import parity.contracts.agents.v1.AgentView;
import parity.contracts.agents.v1.AnswerOfferRequest;
import parity.contracts.agents.v1.AnswerOfferResponse;
import parity.contracts.agents.v1.AttachAgentRequest;
import parity.contracts.agents.v1.AttachAgentResponse;
import parity.contracts.agents.v1.Decision;
import parity.contracts.agents.v1.DetachAgentRequest;
import parity.contracts.agents.v1.DetachReason;
import parity.contracts.agents.v1.EndWrapUpRequest;
import parity.contracts.agents.v1.MembershipTier;
import parity.contracts.agents.v1.Offer;
import parity.contracts.agents.v1.QueueMembership;
import parity.contracts.agents.v1.Reservation;
import parity.contracts.agents.v1.SessionRef;
import parity.contracts.agents.v1.SetStateRequest;
import parity.contracts.agents.v1.TargetState;
import parity.contracts.agents.v1.WatchResponse;
import parity.contracts.interaction.v1.AgentCallView;
import parity.contracts.interaction.v1.BlindTransfer;
import parity.contracts.interaction.v1.CallState;
import parity.contracts.interaction.v1.CancelConsult;
import parity.contracts.interaction.v1.CompleteTransfer;
import parity.contracts.interaction.v1.Consult;
import parity.contracts.interaction.v1.ExecuteRequest;
import parity.contracts.interaction.v1.ExecuteResponse;
import parity.contracts.interaction.v1.GetRequest;
import parity.contracts.interaction.v1.Hangup;
import parity.contracts.interaction.v1.Hold;
import parity.contracts.interaction.v1.Merge;
import parity.contracts.interaction.v1.Mute;
import parity.contracts.interaction.v1.Party;
import parity.contracts.interaction.v1.RemoveParty;
import parity.contracts.interaction.v1.Resume;
import parity.contracts.interaction.v1.Target;
import parity.contracts.interaction.v1.Unmute;
import parity.gateway.agents.CommandIds;
import parity.gateway.agents.GrpcCalls;
import parity.gateway.agents.auth.Identity;
import parity.gateway.agents.auth.TokenVerifier;
import parity.gateway.agents.creds.SipCredentials;
import parity.gateway.agents.creds.TurnCredentials;
import parity.gateway.agents.ws.Frames;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

/**
 * One desktop connection: the single writer (its {@link SerialExecutor}) of the view it shows,
 * merging RE's messages, IE's call views and the desktop's commands. Tenant and agent come from
 * the token only. A lost socket never touches a call: RE is told to detach (the grace starts),
 * and only IE's view says a call ended.
 */
public final class AgentSession {

    private static final Logger log = LoggerFactory.getLogger(AgentSession.class);

    private final Identity identity;
    private final String sessionId = UUID.randomUUID().toString();
    private final WebSocketSession socket;
    private final Outbox out;
    private final SerialExecutor exec;
    private final SessionRegistry registry;
    private final Deps deps;
    private final TokenBucket commands;

    // Owned by the executor.
    private Instant tokenExpires;
    private long generation;
    private long lastDelivery;
    private AgentView agent = AgentView.getDefaultInstance();
    private Offer offer;
    private final Map<String, Reservation> reservations = new HashMap<>();
    private final Map<String, AgentCallView> calls = new HashMap<>();
    private int unansweredPings;
    private long credentialsRenewAtMs;
    private boolean attached;
    private volatile boolean closing;
    private volatile boolean superseded;
    private final java.util.concurrent.atomic.AtomicBoolean removed = new java.util.concurrent.atomic.AtomicBoolean();

    AgentSession(Identity identity, WebSocketSession socket, jakarta.websocket.Session nativeSocket,
            SessionRegistry registry, Deps deps) {
        this.identity = identity;
        this.tokenExpires = identity.expiresAt();
        this.socket = socket;
        this.registry = registry;
        this.deps = deps;
        this.exec = new SerialExecutor("agent-session");
        this.out = new Outbox(nativeSocket, deps.agents().outboxLimit(), code -> close(code, "send_failed"));
        this.commands = new TokenBucket(deps.agents().commandsPerSecond(), deps.agents().commandsPerSecond());
    }

    public Identity identity() {
        return identity;
    }

    boolean markRemoved() {
        return removed.compareAndSet(false, true);
    }

    String key() {
        return SessionRegistry.key(identity.tenantId(), identity.agentId());
    }

    String contact() {
        return "sip:" + identity.agentId() + "@" + identity.tenantId() + "." + deps.agents().sipDomain();
    }

    // ---- entry points (any thread): each queues a task -------------------------------------

    void start() {
        exec.execute(() -> attach(false));
    }

    void reattach() {
        exec.execute(() -> {
            if (attached && !closing) {
                attach(true);
            }
        });
    }

    public void onText(String text) {
        exec.execute(() -> handle(text));
    }

    void onRe(WatchResponse m) {
        exec.execute(() -> handleRe(m));
    }

    void onEvent(String interactionId, long ownerEpoch, long seq) {
        exec.execute(() -> refresh(interactionId, ownerEpoch, seq));
    }

    void tick(long nowMs) {
        exec.execute(() -> housekeeping(nowMs));
    }

    void reconnect(long afterMs) {
        exec.execute(() -> {
            ObjectNode f = Frames.out("reconnect");
            f.put("after_ms", afterMs);
            out.send(f);
        });
    }

    /** The socket closed (any reason): unregister and tell RE, unless a newer session took over. */
    public void socketClosed(int closeCode) {
        closing = true;
        exec.execute(() -> {
            registry.remove(this);
            String detached = null;
            if (attached && !superseded) {
                DetachReason reason = registry.draining() ? DetachReason.DETACH_REASON_DRAIN : DetachReason.DETACH_REASON_SOCKET_CLOSED;
                detach(reason);
                detached = lower(reason.name(), "DETACH_REASON_");
            }
            attached = false;
            at(log.atInfo(), "session.closed").addKeyValue("close_code", closeCode).addKeyValue("detach_reason", detached)
                    .addKeyValue("superseded", superseded).log("desktop session closed");
        });
    }

    /** Another session of the same agent replaced this one (another tab, or RE's Detached). */
    void supersede() {
        superseded = true;
        at(log.atInfo(), "session.superseded").log("another session of the agent took over");
        close(4009, "superseded");
    }

    void close(int code, String reason) {
        closing = true;
        at(log.atInfo(), "session.closing").addKeyValue("close_code", code).addKeyValue("reason", reason)
                .log("closing the desktop session");
        deps.closer().execute(() -> {
            try {
                socket.close(new CloseStatus(code, reason));
            } catch (java.io.IOException | RuntimeException e) {
                at(log.atDebug(), "session.close_failed").addKeyValue("error_kind", e.getClass().getName())
                        .log("closing a socket failed");
            }
        });
    }

    // ---- logs -------------------------------------------------------------------------------

    /** A record about this session: its event, tenant, agent and session. */
    private LoggingEventBuilder at(LoggingEventBuilder b, String event) {
        return b.addKeyValue("event", event).addKeyValue("tenant_id", identity.tenantId())
                .addKeyValue("agent_id", identity.agentId()).addKeyValue("session_id", sessionId);
    }

    /** A record about one of this session's calls: the xsid is the interaction id. */
    private LoggingEventBuilder call(LoggingEventBuilder b, String event, String interactionId) {
        return at(b, event).addKeyValue("xsid", interactionId);
    }

    private static String state(AgentCallView c) {
        return c.getState() == CallState.CALL_STATE_UNSPECIFIED ? "ringing" : lower(c.getState().name(), "CALL_STATE_");
    }

    /** What the desktop was just shown about a call: a state change, or an update within the state. */
    private void shown(AgentCallView before, AgentCallView now, String source) {
        String to = state(now);
        String from = before == null ? "none" : state(before);
        LoggingEventBuilder b = from.equals(to) ? call(log.atInfo(), "call.updated", now.getInteractionId())
                : call(log.atInfo(), "call.state_changed", now.getInteractionId()).addKeyValue("from_state", from)
                        .addKeyValue("to_state", to);
        b.addKeyValue("reservation_id", now.getReservationId().isEmpty() ? null : now.getReservationId())
                .addKeyValue("owner_epoch", now.getOwnerEpoch()).addKeyValue("source", source)
                .addKeyValue("held", now.getHeld()).addKeyValue("muted", now.getMuted())
                .addKeyValue("consulting", now.getConsulting()).addKeyValue("managed", now.getManaged())
                .addKeyValue("parties", now.getPartiesCount())
                .log(from.equals(to) ? "call view sent to the desktop" : "call state sent to the desktop");
    }

    // ---- RE ---------------------------------------------------------------------------------

    private SessionRef ref() {
        return SessionRef.newBuilder().setTenantId(identity.tenantId()).setAgentId(identity.agentId())
                .setSessionId(sessionId).setAttachGeneration(generation).build();
    }

    private void attach(boolean again) {
        AttachAgentResponse r;
        try {
            r = deps.re().attach(AttachAgentRequest.newBuilder().setTenantId(identity.tenantId())
                    .setAgentId(identity.agentId()).setReplicaId(deps.replicaId()).setSessionId(sessionId)
                    .setAgentContact(contact()).build());
        } catch (GrpcCalls.Failed f) {
            deps.metrics().count("attach_failed", f.code());
            at(log.atWarn(), "session.attach_failed").addKeyValue("error_code", f.code()).addKeyValue("again", again)
                    .log("routing-engine refused or missed the attach");
            if (!again) {
                close(4013, "retry_after_ms=" + ThreadLocalRandom.current().nextLong(1000, 5000));
            }
            return;
        }
        if (again && r.getAttachGeneration() != generation) {
            supersede();
            return;
        }
        generation = r.getAttachGeneration();
        attached = true;
        agent = r.getAgent();
        if (again) {
            out.send(agentFrame("agent_state"));
            at(log.atInfo(), "session.reattached").addKeyValue("attach_generation", generation)
                    .log("re-attached to routing-engine");
            fromSnapshot(r);
            return;
        }
        lastDelivery = 0;
        offer = r.hasOffer() ? r.getOffer() : null;
        for (Reservation res : r.getReservationsList()) {
            reservations.put(res.getReservationId(), res);
            registry.index(res.getInteractionId(), this);
            getCall(res.getInteractionId());
        }
        at(log.atInfo(), "session.opened").addKeyValue("attach_generation", generation)
                .addKeyValue("reservations", reservations.size()).addKeyValue("offer", offer != null)
                .log("agent attached");
        ObjectNode snap = Frames.out("snapshot");
        snap.put("session_id", sessionId);
        snap.set("agent", agentJson(agent));
        if (offer != null) {
            snap.set("offer", offerJson(offer, Frames.JSON.createObjectNode()));
            offerLog(offer, "attach");
        }
        ArrayNode res = snap.putArray("reservations");
        ArrayNode wrapUps = snap.putArray("wrap_ups");
        for (Reservation x : reservations.values()) {
            res.add(reservationJson(x, Frames.JSON.createObjectNode()));
            reservationLog(x, "attach");
            if (x.getWrapUp()) {
                wrapUps.addObject().put("reservation_id", x.getReservationId())
                        .put("interaction_id", x.getInteractionId()).put("ends_at_ms", x.getWrapUpEndsAtMs());
            }
        }
        ArrayNode cs = snap.putArray("calls");
        calls.values().forEach(c -> {
            cs.add(callJson(c));
            shown(null, c, "attach");
        });
        out.send(snap);
        sendCredentials(System.currentTimeMillis());
        deps.metrics().count("attached", "ok");
    }

    private void offerLog(Offer o, String source) {
        call(log.atInfo(), "offer.delivered", o.getInteractionId()).addKeyValue("offer_id", o.getOfferId())
                .addKeyValue("queue_id", o.getQueueId().isEmpty() ? null : o.getQueueId())
                .addKeyValue("expires_at_ms", o.getExpiresAtMs()).addKeyValue("consult", o.getConsult())
                .addKeyValue("source", source).log("offer sent to the desktop");
    }

    private void reservationLog(Reservation r, String source) {
        call(log.atInfo(), "reservation.received", r.getInteractionId()).addKeyValue("reservation_id", r.getReservationId())
                .addKeyValue("origin", r.getOrigin() == parity.contracts.agents.v1.ReservationOrigin.RESERVATION_ORIGIN_UNSPECIFIED
                        ? "inbound" : lower(r.getOrigin().name(), "RESERVATION_ORIGIN_"))
                .addKeyValue("wrap_up", r.getWrapUp()).addKeyValue("source", source).log("reservation sent to the desktop");
    }

    private void offerRevokedLog(String interactionId, String offerId, String reason, String source) {
        call(log.atInfo(), "offer.revoked", interactionId).addKeyValue("offer_id", offerId).addKeyValue("reason", reason)
                .addKeyValue("source", source).log("offer withdrawn from the desktop");
    }

    /**
     * CR-053 (the gateway's part): after a re-attach RE's snapshot is the truth. A message lost on
     * the way (a stream that dropped before its replay, or a replay overtaken) leaves the desktop
     * without an offer or a reservation it has: both are taken from the snapshot, and an offer
     * shown that RE no longer has is withdrawn.
     */
    private void fromSnapshot(AttachAgentResponse r) {
        Offer live = r.hasOffer() ? r.getOffer() : null;
        if (live != null && (offer == null || !offer.getOfferId().equals(live.getOfferId()))) {
            offer = live;
            out.send(offerJson(offer, Frames.out("offer")));
            deps.metrics().offerDelivered();
            offerLog(offer, "reattach");
            deps.metrics().count("offer_from_snapshot", "reattach");
        } else if (live == null && offer != null) {
            // The message that ended it was lost; its reason isn't in the snapshot: past its expiry
            // it expired, otherwise the caller left (the only other way an unanswered offer goes
            // while the agent stays attached).
            ObjectNode f = Frames.out("offer_revoked");
            f.put("offer_id", offer.getOfferId());
            String reason = System.currentTimeMillis() >= offer.getExpiresAtMs() ? "expired" : "caller_gone";
            f.put("reason", reason);
            out.send(f);
            offerRevokedLog(offer.getInteractionId(), offer.getOfferId(), reason, "reattach");
            offer = null;
        }
        for (Reservation x : r.getReservationsList()) {
            if (reservations.putIfAbsent(x.getReservationId(), x) == null) {
                registry.index(x.getInteractionId(), this);
                out.send(reservationJson(x, Frames.out("reserved")));
                deps.metrics().count("reservation_from_snapshot", "reattach");
                reservationLog(x, "reattach");
                if (x.getWrapUp()) {
                    ObjectNode w = Frames.out("wrap_up");
                    w.put("reservation_id", x.getReservationId());
                    w.put("interaction_id", x.getInteractionId());
                    w.put("ends_at_ms", x.getWrapUpEndsAtMs());
                    out.send(w);
                    call(log.atInfo(), "wrap_up.started", x.getInteractionId()).addKeyValue("reservation_id", x.getReservationId())
                            .addKeyValue("ends_at_ms", x.getWrapUpEndsAtMs()).addKeyValue("source", "reattach")
                            .log("wrap-up sent to the desktop");
                }
            }
        }
    }

    private void detach(DetachReason reason) {
        try {
            deps.re().detach(DetachAgentRequest.newBuilder().setSession(ref()).setReason(reason).build());
        } catch (GrpcCalls.Failed f) {
            deps.metrics().count("detach_failed", f.code());
            at(log.atWarn(), "session.detach_failed").addKeyValue("error_code", f.code())
                    .addKeyValue("reason", lower(reason.name(), "DETACH_REASON_")).log("routing-engine missed the detach");
        }
    }

    private void handleRe(WatchResponse m) {
        if (m.getAttachGeneration() != generation || m.getDeliverySeq() <= lastDelivery) {
            registry.ack(m);
            return;
        }
        lastDelivery = m.getDeliverySeq();
        switch (m.getEventCase()) {
            case OFFER -> {
                offer = m.getOffer();
                out.send(offerJson(offer, Frames.out("offer")));
                deps.metrics().offerDelivered();
                offerLog(offer, "watch");
            }
            case OFFER_REVOKED -> {
                String interactionId = null;
                if (offer != null && offer.getOfferId().equals(m.getOfferRevoked().getOfferId())) {
                    interactionId = offer.getInteractionId();
                    offer = null;
                }
                ObjectNode f = Frames.out("offer_revoked");
                f.put("offer_id", m.getOfferRevoked().getOfferId());
                String reason = lower(m.getOfferRevoked().getReason().name(), "REVOKE_REASON_");
                f.put("reason", reason);
                out.send(f);
                if (interactionId != null) {
                    offerRevokedLog(interactionId, m.getOfferRevoked().getOfferId(), reason, "watch");
                } else {
                    at(log.atDebug(), "offer.revoke_unmatched").addKeyValue("offer_id", m.getOfferRevoked().getOfferId())
                            .addKeyValue("reason", reason).log("a withdrawn offer the desktop wasn't shown");
                }
            }
            case RESERVED -> {
                Reservation r = m.getReserved();
                reservations.put(r.getReservationId(), r);
                registry.index(r.getInteractionId(), this);
                out.send(reservationJson(r, Frames.out("reserved")));
                reservationLog(r, "watch");
            }
            case STATE_CHANGED -> {
                if (m.getStateChanged().getAgent().getStateVersion() >= agent.getStateVersion()) {
                    agent = m.getStateChanged().getAgent();
                    out.send(agentFrame("agent_state"));
                }
            }
            case WRAP_UP_STARTED -> {
                var w = m.getWrapUpStarted();
                Reservation r = reservations.get(w.getReservationId());
                if (r != null) {
                    reservations.put(r.getReservationId(), r.toBuilder().setWrapUp(true).setWrapUpEndsAtMs(w.getEndsAtMs()).build());
                }
                ObjectNode f = Frames.out("wrap_up");
                f.put("reservation_id", w.getReservationId());
                f.put("interaction_id", w.getInteractionId());
                f.put("ends_at_ms", w.getEndsAtMs());
                out.send(f);
                call(log.atInfo(), "wrap_up.started", w.getInteractionId()).addKeyValue("reservation_id", w.getReservationId())
                        .addKeyValue("ends_at_ms", w.getEndsAtMs()).addKeyValue("source", "watch")
                        .log("wrap-up sent to the desktop");
            }
            case QUEUES_CHANGED -> {
                // contracts 0.11.0: RE moved the agent between queues (applied now, or at the next offer).
                var q = m.getQueuesChanged();
                if (q.getQueuesVersion() >= agent.getQueuesVersion()) {
                    agent = agent.toBuilder().clearQueues().addAllQueues(q.getQueuesList()).clearPendingQueues()
                            .addAllPendingQueues(q.getPendingQueuesList()).setQueuesVersion(q.getQueuesVersion()).build();
                    ObjectNode f = Frames.out("queues");
                    f.set("queues", membershipJson(q.getQueuesList()));
                    if (q.getPendingQueuesCount() > 0) {
                        f.set("pending_queues", membershipJson(q.getPendingQueuesList()));
                    }
                    f.put("queues_version", q.getQueuesVersion());
                    f.put("reason", lower(q.getReason().name(), "MEMBERSHIP_REASON_"));
                    out.send(f);
                }
            }
            case DETACHED -> {
                attached = false;
                registry.ack(m);
                supersede();
                return;
            }
            case HEARTBEAT, EVENT_NOT_SET -> {
                // nothing for a desktop
            }
        }
        registry.ack(m);
    }

    // ---- IE ---------------------------------------------------------------------------------

    /** IE's view of a call, after an event newer than the one shown. */
    private void refresh(String interactionId, long ownerEpoch, long seq) {
        AgentCallView shown = calls.get(interactionId);
        if (shown != null && (shown.getOwnerEpoch() > ownerEpoch
                || (shown.getOwnerEpoch() == ownerEpoch && shown.getSeq() >= seq))) {
            return;
        }
        getCall(interactionId);
        AgentCallView now = calls.get(interactionId);
        if (now != null) {
            out.send(callFrame(now));
            shown(shown, now, "event");
            if (now.getState() == CallState.CALL_STATE_ENDED) {
                calls.remove(interactionId);
            }
        }
    }

    private void getCall(String interactionId) {
        try {
            AgentCallView v = deps.ie().get(GetRequest.newBuilder().setTenantId(identity.tenantId())
                    .setInteractionId(interactionId).setAgentId(identity.agentId()).build()).getCall();
            calls.put(interactionId, v);
        } catch (GrpcCalls.Failed f) {
            call(log.atWarn(), "call.view_failed", interactionId).addKeyValue("error_code", f.code())
                    .log("interaction-engine didn't give the call's view");
            if ("not_your_call".equals(f.code()) || "not_found".equals(f.code())) {
                AgentCallView gone = AgentCallView.newBuilder().setInteractionId(interactionId)
                        .setState(CallState.CALL_STATE_ENDED).setManaged(true).build();
                calls.put(interactionId, gone);
            }
            deps.metrics().count("ie_get_failed", f.code());
        }
    }

    private boolean knows(String interactionId) {
        if (calls.containsKey(interactionId)) {
            return true;
        }
        return reservations.values().stream().anyMatch(r -> r.getInteractionId().equals(interactionId));
    }

    private String reservationFor(String interactionId) {
        return reservations.values().stream().filter(r -> r.getInteractionId().equals(interactionId))
                .map(Reservation::getReservationId).findFirst().orElse("");
    }

    // ---- the desktop's frames ---------------------------------------------------------------

    private void handle(String text) {
        Frames.Frame f;
        try {
            f = Frames.parse(text);
        } catch (Frames.Invalid e) {
            deps.metrics().count("frame_invalid", "schema");
            at(log.atWarn(), "frame.refused").addKeyValue("error_code", "bad_request").log("a desktop frame was refused");
            if (e.id() != null) {
                result(e.id(), "bad_request", false);
            }
            return;
        }
        if (f.type() == null) {
            deps.metrics().count("frame_ignored", "unknown_type");
            at(log.atDebug(), "frame.ignored").addKeyValue("reason", "unknown_type").log("a desktop frame of an unknown type");
            return;
        }
        if (!attached && !"pong".equals(f.type())) {
            at(log.atWarn(), "frame.refused").addKeyValue("error_code", "unavailable").addKeyValue("frame", f.type())
                    .log("a desktop frame was refused");
            if (f.id() != null) {
                result(f.id(), "unavailable", true);
            }
            return;
        }
        switch (f.type()) {
            case "pong" -> unansweredPings = 0;
            case "auth" -> refreshToken(f.text("token"));
            default -> {
                if (!commands.tryTake(System.nanoTime())) {
                    at(log.atWarn(), "frame.refused").addKeyValue("error_code", "rate_limited").addKeyValue("frame", f.type())
                            .log("a desktop frame was refused");
                    result(f.id(), "rate_limited", true);
                    return;
                }
                command(f);
            }
        }
    }

    private void refreshToken(String token) {
        try {
            Identity again = deps.tokens().verify(token);
            if (!again.tenantId().equals(identity.tenantId()) || !again.agentId().equals(identity.agentId())) {
                close(4003, "identity_changed");
                return;
            }
            tokenExpires = again.expiresAt();
            at(log.atDebug(), "session.token_renewed").log("the desktop renewed its token");
        } catch (TokenVerifier.Refused e) {
            at(log.atWarn(), "session.token_refused").addKeyValue("error_code", e.code()).log("the desktop's new token was refused");
            close(4001, "auth_failed");
        }
    }

    private void command(Frames.Frame f) {
        String commandId = CommandIds.derive(identity.tenantId(), identity.agentId(), f.id());
        // The call the command is about, when this session knows it (the offer shown, a reservation's call).
        String xsid = switch (f.type()) {
            case "accept", "reject" -> offer != null && offer.getOfferId().equals(f.text("offer_id")) ? offer.getInteractionId() : null;
            case "wrap_up_done" -> {
                Reservation r = reservations.get(f.text("reservation_id"));
                yield r == null ? null : r.getInteractionId();
            }
            case "set_state" -> null;
            default -> knows(f.text("interaction_id")) ? f.text("interaction_id") : null;
        };
        long started = System.nanoTime();
        try {
            switch (f.type()) {
                case "set_state" -> {
                    TargetState t = switch (f.text("state")) {
                        case "ready" -> TargetState.TARGET_STATE_READY;
                        case "not_ready" -> TargetState.TARGET_STATE_NOT_READY;
                        default -> TargetState.TARGET_STATE_LOGGED_OUT;
                    };
                    var b = SetStateRequest.newBuilder().setSession(ref()).setCommandId(commandId).setTarget(t);
                    if (f.text("reason_code") != null) {
                        b.setReasonCode(f.text("reason_code"));
                    }
                    agentUpdate(deps.re().setState(b.build()).getAgent());
                    at(log.atInfo(), "agent.command_executed").addKeyValue("command_id", commandId).addKeyValue("op", "set_state")
                            .addKeyValue("target", f.text("state")).addKeyValue("reason_code", f.text("reason_code"))
                            .addKeyValue("outcome", "ok").addKeyValue("duration_ms", (System.nanoTime() - started) / 1_000_000)
                            .log("the agent's state command was carried out");
                }
                case "accept", "reject" -> {
                    var b = AnswerOfferRequest.newBuilder().setSession(ref()).setCommandId(commandId)
                            .setOfferId(f.text("offer_id"))
                            .setDecision("accept".equals(f.type()) ? Decision.DECISION_ACCEPT : Decision.DECISION_REJECT);
                    if (f.text("reason") != null) {
                        b.setRejectReason(f.text("reason"));
                    }
                    AnswerOfferResponse r = deps.re().answerOffer(b.build());
                    if (offer != null && offer.getOfferId().equals(f.text("offer_id"))) {
                        offer = null;
                    }
                    if (r.hasReservation()) {
                        reservations.put(r.getReservation().getReservationId(), r.getReservation());
                        registry.index(r.getReservation().getInteractionId(), this);
                    }
                    String id = xsid != null ? xsid : r.hasReservation() ? r.getReservation().getInteractionId() : null;
                    if (id != null) {
                        call(log.atInfo(), "offer.answered", id).addKeyValue("command_id", commandId)
                                .addKeyValue("offer_id", f.text("offer_id")).addKeyValue("answer", f.type())
                                .addKeyValue("reservation_id", r.hasReservation() ? r.getReservation().getReservationId() : null)
                                .addKeyValue("outcome", "ok").addKeyValue("duration_ms", (System.nanoTime() - started) / 1_000_000)
                                .log("the desktop answered an offer");
                    } else {
                        at(log.atInfo(), "agent.command_executed").addKeyValue("command_id", commandId).addKeyValue("op", f.type())
                                .addKeyValue("offer_id", f.text("offer_id")).addKeyValue("outcome", "ok")
                                .log("the desktop answered an offer it wasn't shown here");
                    }
                }
                case "wrap_up_done" -> {
                    agentUpdate(deps.re().endWrapUp(EndWrapUpRequest.newBuilder().setSession(ref())
                            .setCommandId(commandId).setReservationId(f.text("reservation_id"))
                            .setDispositionCode(f.text("disposition_code")).build()).getAgent());
                    Reservation done = reservations.remove(f.text("reservation_id"));
                    if (done != null && reservations.values().stream()
                            .noneMatch(r -> r.getInteractionId().equals(done.getInteractionId()))) {
                        calls.remove(done.getInteractionId());
                        registry.unindex(done.getInteractionId(), this);
                    }
                    if (xsid != null) {
                        call(log.atInfo(), "wrap_up.ended", xsid).addKeyValue("command_id", commandId)
                                .addKeyValue("reservation_id", f.text("reservation_id"))
                                .addKeyValue("disposition_code", f.text("disposition_code")).addKeyValue("outcome", "ok")
                                .log("the agent ended the wrap-up");
                    } else {
                        at(log.atInfo(), "agent.command_executed").addKeyValue("command_id", commandId)
                                .addKeyValue("op", "wrap_up_done").addKeyValue("outcome", "ok")
                                .log("the agent ended a wrap-up this session didn't show");
                    }
                }
                default -> control(f, commandId);
            }
            result(f.id(), null, false);
        } catch (GrpcCalls.Failed e) {
            String code = switch (e.code()) {
                case "offer_gone", "not_your_call", "invalid_state", "call_unmanaged", "destination_not_allowed",
                        "not_found", "conflict", "invalid_transition" -> e.code();
                case "session_superseded" -> {
                    supersede();
                    yield "unavailable";
                }
                default -> "unavailable";
            };
            deps.metrics().count("command_refused", code);
            (xsid != null ? call(log.atInfo(), "call.command_refused", xsid) : at(log.atInfo(), "agent.command_refused"))
                    .addKeyValue("command_id", commandId).addKeyValue("op", f.type()).addKeyValue("outcome", "refused")
                    .addKeyValue("error_code", e.code()).addKeyValue("answered", code)
                    .addKeyValue("duration_ms", (System.nanoTime() - started) / 1_000_000)
                    .log("a desktop command was refused");
            result(f.id(), code, "unavailable".equals(code));
        }
    }

    private void control(Frames.Frame f, String commandId) throws GrpcCalls.Failed {
        String interactionId = f.text("interaction_id");
        if (!knows(interactionId)) {
            // Defence in depth: IE refuses it too (the authorization rule).
            throw new GrpcCalls.Failed(io.grpc.Status.Code.PERMISSION_DENIED, "not_your_call");
        }
        ExecuteRequest.Builder b = ExecuteRequest.newBuilder().setTenantId(identity.tenantId())
                .setInteractionId(interactionId).setAgentId(identity.agentId())
                .setReservationId(reservationFor(interactionId)).setCommandId(commandId);
        switch (f.type()) {
            case "hold" -> b.setHold(Hold.getDefaultInstance());
            case "resume" -> b.setResume(Resume.getDefaultInstance());
            case "mute" -> b.setMute(Mute.getDefaultInstance());
            case "unmute" -> b.setUnmute(Unmute.getDefaultInstance());
            case "hangup" -> b.setHangup(Hangup.getDefaultInstance());
            case "merge" -> b.setMerge(Merge.getDefaultInstance());
            case "complete_transfer" -> b.setCompleteTransfer(CompleteTransfer.getDefaultInstance());
            case "cancel_consult" -> b.setCancelConsult(CancelConsult.getDefaultInstance());
            case "remove_party" -> b.setRemoveParty(RemoveParty.newBuilder().setLegId(f.text("leg_id")));
            case "transfer" -> b.setBlindTransfer(BlindTransfer.newBuilder().setTarget(target(f.body().get("target"))));
            case "consult" -> b.setConsult(Consult.newBuilder().setTarget(target(f.body().get("target"))));
            default -> throw new GrpcCalls.Failed(io.grpc.Status.Code.INVALID_ARGUMENT, "invalid_request");
        }
        long started = System.nanoTime();
        ExecuteResponse r = deps.ie().execute(b.build());
        call(log.atInfo(), "call.command_executed", interactionId).addKeyValue("command_id", commandId).addKeyValue("op", f.type())
                .addKeyValue("reservation_id", b.getReservationId().isEmpty() ? null : b.getReservationId())
                .addKeyValue("outcome", "ok").addKeyValue("duration_ms", (System.nanoTime() - started) / 1_000_000)
                .log("a call command was carried out");
        if (r.hasCall()) {
            AgentCallView before = calls.put(interactionId, r.getCall());
            out.send(callFrame(r.getCall()));
            shown(before, r.getCall(), "command");
        }
    }

    private static Target target(JsonNode t) {
        Target.Builder b = Target.newBuilder();
        if (t.has("queue_id")) {
            b.setQueueId(t.get("queue_id").asString());
        } else if (t.has("agent_id")) {
            b.setAgentId(t.get("agent_id").asString());
        } else {
            b.setNumber(t.get("number").asString());
        }
        return b.build();
    }

    private void agentUpdate(AgentView v) {
        if (v.getStateVersion() >= agent.getStateVersion()) {
            agent = v;
            out.send(agentFrame("agent_state"));
        }
    }

    private void result(String id, String error, boolean retry) {
        ObjectNode f = Frames.out("result");
        f.put("id", id);
        f.put("ok", error == null);
        if (error != null) {
            f.putObject("error").put("code", error).put("retry", retry);
        }
        out.send(f);
    }

    // ---- housekeeping -----------------------------------------------------------------------

    private void housekeeping(long nowMs) {
        if (closing) {
            return;
        }
        if (nowMs > tokenExpires.toEpochMilli()) {
            close(4001, "token_expired");
            return;
        }
        if (unansweredPings >= 2) {
            close(1001, "ping_timeout");
            return;
        }
        unansweredPings++;
        out.send(Frames.out("ping"));
        if (attached && nowMs >= credentialsRenewAtMs) {
            sendCredentials(nowMs);
        }
    }

    private void sendCredentials(long nowMs) {
        TurnCredentials.Grant ice = deps.turn().issue(identity.tenantId(), identity.agentId());
        ObjectNode f = Frames.out("ice_servers");
        ArrayNode servers = f.putArray("ice_servers");
        for (TurnCredentials.IceServer s : ice.servers()) {
            ObjectNode o = servers.addObject();
            s.urls().forEach(o.putArray("urls")::add);
            if (s.username() != null) {
                o.put("username", s.username());
                o.put("credential", s.credential());
            }
        }
        f.put("expires_at_ms", ice.expiresAtMs());
        out.send(f);
        SipCredentials.Grant sip = deps.sip().issue(identity.tenantId(), identity.agentId(), contact(), sessionId);
        long expiresAtMs = ice.expiresAtMs();
        if (sip != null) {
            ObjectNode s = Frames.out("sip_credentials");
            s.put("uri", contact());
            deps.sipWsServers().forEach(s.putArray("ws_servers")::add);
            s.put("token", sip.token());
            s.put("expires_at_ms", sip.expiresAtMs());
            out.send(s);
            expiresAtMs = Math.min(expiresAtMs, sip.expiresAtMs());
        }
        credentialsRenewAtMs = nowMs + (expiresAtMs - nowMs) / 2;
    }

    // ---- JSON for the desktop ---------------------------------------------------------------

    static String lower(String name, String prefix) {
        return name.substring(prefix.length()).toLowerCase(Locale.ROOT);
    }

    private ObjectNode agentFrame(String type) {
        ObjectNode f = Frames.out(type);
        f.set("agent", agentJson(agent));
        return f;
    }

    private static ObjectNode agentJson(AgentView a) {
        ObjectNode o = Frames.JSON.createObjectNode();
        o.put("state", a.getState() == parity.contracts.agents.v1.AgentState.AGENT_STATE_UNSPECIFIED ? "logged_out"
                : lower(a.getState().name(), "AGENT_STATE_"));
        if (!a.getReasonCode().isEmpty()) {
            o.put("reason_code", a.getReasonCode());
        }
        if (a.getPendingTarget() != TargetState.TARGET_STATE_UNSPECIFIED) {
            o.put("pending_target", lower(a.getPendingTarget().name(), "TARGET_STATE_"));
            if (!a.getPendingReasonCode().isEmpty()) {
                o.put("pending_reason_code", a.getPendingReasonCode());
            }
        }
        o.put("since_ms", a.getSinceMs());
        o.put("state_version", a.getStateVersion());
        o.put("mode", a.getMode() == parity.contracts.agents.v1.Mode.MODE_CAMPAIGN ? "campaign" : "inbound");
        if (!a.getCampaignId().isEmpty()) {
            o.put("campaign_id", a.getCampaignId());
        }
        if (a.getQueuesCount() > 0 || a.getQueuesVersion() > 0) {  // contracts 0.11.0
            o.set("queues", membershipJson(a.getQueuesList()));
            if (a.getPendingQueuesCount() > 0) {
                o.set("pending_queues", membershipJson(a.getPendingQueuesList()));
            }
            o.put("queues_version", a.getQueuesVersion());
        }
        return o;
    }

    private static ArrayNode membershipJson(java.util.List<QueueMembership> list) {
        ArrayNode a = Frames.JSON.createArrayNode();
        for (QueueMembership q : list) {
            ObjectNode n = a.addObject();
            n.put("queue_id", q.getQueueId());
            n.put("tier", q.getTier() == MembershipTier.MEMBERSHIP_TIER_SECONDARY ? "secondary" : "primary");
            if (q.getPinned()) {
                n.put("pinned", true);
            }
            n.put("since_ms", q.getSinceMs());
        }
        return a;
    }

    private static ObjectNode offerJson(Offer o, ObjectNode n) {
        n.put("offer_id", o.getOfferId());
        n.put("interaction_id", o.getInteractionId());
        n.put("channel", o.getChannel() == parity.contracts.agents.v1.Channel.CHANNEL_UNSPECIFIED ? "voice"
                : lower(o.getChannel().name(), "CHANNEL_"));
        if (!o.getQueueId().isEmpty()) {
            n.put("queue_id", o.getQueueId());
        }
        n.put("expires_at_ms", o.getExpiresAtMs());
        n.put("consult", o.getConsult());
        n.put("direct", o.getDirect());
        if (o.getTier() != MembershipTier.MEMBERSHIP_TIER_UNSPECIFIED) {  // contracts 0.11.0
            n.put("tier", o.getTier() == MembershipTier.MEMBERSHIP_TIER_SECONDARY ? "secondary" : "primary");
        }
        if (o.getCallback()) {
            n.put("callback", true);
        }
        return n;
    }

    private static ObjectNode reservationJson(Reservation r, ObjectNode n) {
        n.put("reservation_id", r.getReservationId());
        n.put("interaction_id", r.getInteractionId());
        n.put("origin", r.getOrigin() == parity.contracts.agents.v1.ReservationOrigin.RESERVATION_ORIGIN_UNSPECIFIED
                ? "inbound" : lower(r.getOrigin().name(), "RESERVATION_ORIGIN_"));
        if (!r.getCampaignId().isEmpty()) {
            n.put("campaign_id", r.getCampaignId());
        }
        return n;
    }

    private static ObjectNode callFrame(AgentCallView c) {
        ObjectNode f = Frames.out("call");
        f.set("call", callJson(c));
        return f;
    }

    static ObjectNode callJson(AgentCallView c) {
        ObjectNode o = Frames.JSON.createObjectNode();
        o.put("interaction_id", c.getInteractionId());
        if (!c.getReservationId().isEmpty()) {
            o.put("reservation_id", c.getReservationId());
        }
        o.put("state", c.getState() == CallState.CALL_STATE_UNSPECIFIED ? "ringing" : lower(c.getState().name(), "CALL_STATE_"));
        o.put("held", c.getHeld());
        o.put("muted", c.getMuted());
        o.put("consulting", c.getConsulting());
        o.put("recording", c.getRecording() == parity.contracts.interaction.v1.RecordingState.RECORDING_STATE_UNSPECIFIED
                ? "off" : lower(c.getRecording().name(), "RECORDING_STATE_"));
        o.put("managed", c.getManaged());
        ArrayNode parties = o.putArray("parties");
        for (Party p : c.getPartiesList()) {
            ObjectNode n = parties.addObject();
            n.put("role", p.getRole() == parity.contracts.interaction.v1.PartyRole.PARTY_ROLE_UNSPECIFIED ? "external"
                    : lower(p.getRole().name(), "PARTY_ROLE_"));
            if (!p.getAgentId().isEmpty()) {
                n.put("agent_id", p.getAgentId());
            }
            n.put("leg_id", p.getLegId());
            n.put("live", p.getLive());
        }
        return o;
    }
}
