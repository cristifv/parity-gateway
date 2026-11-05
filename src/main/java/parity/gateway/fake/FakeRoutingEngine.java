package parity.gateway.fake;

import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;
import parity.contracts.agents.v1.AcknowledgeRequest;
import parity.contracts.agents.v1.AcknowledgeResponse;
import parity.contracts.agents.v1.AgentServiceGrpc;
import parity.contracts.agents.v1.AgentState;
import parity.contracts.agents.v1.AgentView;
import parity.contracts.agents.v1.AnswerOfferRequest;
import parity.contracts.agents.v1.AnswerOfferResponse;
import parity.contracts.agents.v1.AttachAgentRequest;
import parity.contracts.agents.v1.AttachAgentResponse;
import parity.contracts.agents.v1.Channel;
import parity.contracts.agents.v1.Decision;
import parity.contracts.agents.v1.DetachAgentRequest;
import parity.contracts.agents.v1.DetachAgentResponse;
import parity.contracts.agents.v1.DetachReason;
import parity.contracts.agents.v1.Detached;
import parity.contracts.agents.v1.DetachedReason;
import parity.contracts.agents.v1.EndWrapUpRequest;
import parity.contracts.agents.v1.EndWrapUpResponse;
import parity.contracts.agents.v1.Heartbeat;
import parity.contracts.agents.v1.Mode;
import parity.contracts.agents.v1.Offer;
import parity.contracts.agents.v1.OfferRevoked;
import parity.contracts.agents.v1.Reservation;
import parity.contracts.agents.v1.ReservationOrigin;
import parity.contracts.agents.v1.RevokeReason;
import parity.contracts.agents.v1.SessionRef;
import parity.contracts.agents.v1.SetStateRequest;
import parity.contracts.agents.v1.SetStateResponse;
import parity.contracts.agents.v1.StateChanged;
import parity.contracts.agents.v1.TargetState;
import parity.contracts.agents.v1.WatchRequest;
import parity.contracts.agents.v1.WatchResponse;
import parity.contracts.agents.v1.WrapUpStarted;
import parity.contracts.routing.v1.Assigned;
import parity.contracts.routing.v1.CallEndedRequest;
import parity.contracts.routing.v1.CallEndedResponse;
import parity.contracts.routing.v1.DequeueRequest;
import parity.contracts.routing.v1.DequeueResponse;
import parity.contracts.routing.v1.EnqueueRequest;
import parity.contracts.routing.v1.EnqueueResponse;
import parity.contracts.routing.v1.EventTarget;
import parity.contracts.routing.v1.Outcome;
import parity.contracts.routing.v1.QueueEntry;
import parity.contracts.routing.v1.ReportOutcomeRequest;
import parity.contracts.routing.v1.ReportOutcomeResponse;
import parity.contracts.routing.v1.RoutingServiceGrpc;
import parity.contracts.routing.v1.WrapUpEnded;

/**
 * A fake routing-engine with one agent registry serving both halves (PLAN.md question 2):
 * {@code parity.agents.v1.AgentService} for the gateway and a minimal
 * {@code parity.routing.v1.RoutingService} for IE (Enqueue, Dequeue, ReportOutcome, CallEnded,
 * Watch with Assigned and WrapUpEnded; the rest UNIMPLEMENTED, GW-013). Longest-idle routing, a
 * 15 s offer timeout, a 60 s grace for detached agents. In memory, one lock, a clock the tests
 * can drive ({@link #tick()}).
 */
public final class FakeRoutingEngine implements AutoCloseable {

    /** One agent in the registry. */
    private static final class Agent {
        final String tenant;
        final String id;
        AgentState state = AgentState.AGENT_STATE_LOGGED_OUT;
        String reason = "";
        TargetState pending = TargetState.TARGET_STATE_UNSPECIFIED;
        String pendingReason = "";
        long sinceMs;
        long version;
        String sessionId = "";
        String replica = "";
        String contact = "";
        long generation;
        boolean attached;
        long detachedAtMs;
        long seq;
        OfferRec offer;
        final Map<String, Reservation> reservations = new LinkedHashMap<>();
        final Set<String> rejected = new HashSet<>();

        Agent(String tenant, String id) {
            this.tenant = tenant;
            this.id = id;
        }
    }

    private record OfferRec(String offerId, Waiting call, long expiresAtMs) {}

    /** A queued interaction. */
    private record Waiting(String tenant, String interactionId, String queueId, long orderingAtMs, String entryId,
            long ownerEpoch) {}

    private record Remembered(com.google.protobuf.ByteString request, Object response, Status.Code status, String code) {}

    private final ReentrantLock lock = new ReentrantLock();
    private final Map<String, Agent> agents = new HashMap<>();
    private final Deque<Waiting> queue = new ArrayDeque<>();
    private final Map<String, ServerCallStreamObserver<WatchResponse>> agentStreams = new HashMap<>();
    private final List<ServerCallStreamObserver<parity.contracts.routing.v1.WatchResponse>> ieStreams = new ArrayList<>();
    private final Map<String, Remembered> commands = new HashMap<>();
    /** Agents (tenant/agent) whose next Offer message is lost on the way (CR-053's case). */
    private final Set<String> loseOffer = new HashSet<>();
    private final AtomicLong ieSeq = new AtomicLong();
    /** Offers that expired unanswered (GW-036: a lost offer that the expiry healed). */
    private final AtomicInteger offersExpired = new AtomicInteger();
    private final LongSupplier clock;
    private final long offerTimeoutMs;
    private final long graceMs;
    private final long wrapUpMs = 60_000;
    private long leaderEpoch = 7;
    private Server server;
    private ScheduledExecutorService ticker;

    public FakeRoutingEngine(LongSupplier clock, long offerTimeoutMs, long graceMs) {
        this.clock = clock;
        this.offerTimeoutMs = offerTimeoutMs;
        this.graceMs = graceMs;
    }

    /** Serves on {@code port} (0: any) and ticks every 100 ms when {@code realTime}. */
    public FakeRoutingEngine start(int port, boolean realTime) throws IOException {
        server = NettyServerBuilder.forPort(port)
                .addService(ServerInterceptors.intercept(new Agents(), FakeSupport.callers()))
                .addService(ServerInterceptors.intercept(new Routing(), FakeSupport.callers()))
                .build().start();
        if (realTime) {
            ticker = Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().name("fake-re-tick").daemon(true).factory());
            var unused = ticker.scheduleAtFixedRate(this::tick, 100, 100, TimeUnit.MILLISECONDS);
        }
        return this;
    }

    public int port() {
        return server.getPort();
    }

    // ---- what IE (or a test standing in for it) does --------------------------------------

    /** A call joins a queue; the longest-idle ready agent gets the offer. */
    public void queueCall(String tenant, String interactionId, String queueId) {
        locked(() -> {
            queue.addLast(new Waiting(tenant, interactionId, queueId, clock.getAsLong(), UUID.randomUUID().toString(), 1));
            dispatch();
        });
    }

    /** IE's CallEnded for a reservation: wrap-up starts. */
    public void callEnded(String tenant, String reservationId) {
        locked(() -> {
            for (Agent a : agents.values()) {
                Reservation r = a.reservations.get(reservationId);
                if (r != null && a.tenant.equals(tenant)) {
                    long ends = clock.getAsLong() + wrapUpMs;
                    a.reservations.put(reservationId, r.toBuilder().setWrapUp(true).setWrapUpEndsAtMs(ends).build());
                    change(a, AgentState.AGENT_STATE_WRAP_UP, a.reason);
                    send(a, WatchResponse.newBuilder().setWrapUpStarted(WrapUpStarted.newBuilder()
                            .setReservationId(reservationId).setInteractionId(r.getInteractionId()).setEndsAtMs(ends)));
                }
            }
        });
    }

    /** Seeds an agent (the scripts' {@code given}). */
    public void seedAgent(String tenant, String agentId, AgentState state, String reason, Reservation reservation) {
        locked(() -> {
            Agent a = agent(tenant, agentId);
            a.state = state;
            a.reason = reason == null ? "" : reason;
            a.sinceMs = clock.getAsLong();
            a.version = switch (state) {
                case AGENT_STATE_READY -> 2;
                case AGENT_STATE_ON_CALL -> 5;
                default -> 1;
            };
            if (reservation != null) {
                a.reservations.put(reservation.getReservationId(), reservation);
            }
        });
    }

    public void tick() {
        locked(() -> {
            long now = clock.getAsLong();
            for (Agent a : agents.values()) {
                if (a.offer != null && now >= a.offer.expiresAtMs()) {
                    offersExpired.incrementAndGet();
                    OfferRec o = a.offer;
                    revoke(a, RevokeReason.REVOKE_REASON_EXPIRED);
                    queue.addFirst(o.call());
                    change(a, AgentState.AGENT_STATE_NOT_READY, "missed_offer");
                }
                if (!a.attached && a.detachedAtMs > 0 && now - a.detachedAtMs >= graceMs) {
                    a.detachedAtMs = 0;
                    ask(a, TargetState.TARGET_STATE_NOT_READY, "disconnected");
                }
            }
            for (ServerCallStreamObserver<WatchResponse> s : agentStreams.values()) {
                s.onNext(WatchResponse.newBuilder().setLeaderEpoch(leaderEpoch)
                        .setHeartbeat(Heartbeat.newBuilder().setAtMs(now)).build());
            }
            dispatch();
        });
    }

    /**
     * The agent's next Offer message is lost on the way (as a stream that dropped before its
     * replay, CR-053): RE holds the offer, the desktop never hears of it until a re-attach.
     */
    public void loseNextOffer(String tenant, String agentId) {
        locked(() -> loseOffer.add(tenant + "/" + agentId));
    }

    /** A new leader: messages carry a higher epoch (the gateway re-attaches its sessions). */
    public void newLeader() {
        locked(() -> {
            leaderEpoch++;
        });
    }

    /** Ends every Watch stream, as a restart or a network cut would. */
    public void dropStreams() {
        locked(() -> {
            agentStreams.values().forEach(s -> s.onError(Status.UNAVAILABLE.asRuntimeException()));
            agentStreams.clear();
        });
    }

    public AgentState stateOf(String tenant, String agentId) {
        lock.lock();
        try {
            Agent a = agents.get(tenant + "/" + agentId);
            return a == null ? AgentState.AGENT_STATE_UNSPECIFIED : a.state;
        } finally {
            lock.unlock();
        }
    }

    /** Offers that expired unanswered so far (each puts its agent NOT_READY {@code missed_offer}). */
    public int offersExpired() {
        return offersExpired.get();
    }

    /** The tenant's agents now NOT_READY with reason {@code missed_offer}. */
    public int missedOffer(String tenant) {
        lock.lock();
        try {
            return (int) agents.values().stream().filter(a -> a.tenant.equals(tenant)
                    && a.state == AgentState.AGENT_STATE_NOT_READY && "missed_offer".equals(a.reason)).count();
        } finally {
            lock.unlock();
        }
    }

    /** Reservations made for the tenant's agents (each accepted offer makes one), any state. */
    public int reservations(String tenant) {
        lock.lock();
        try {
            return agents.values().stream().filter(a -> a.tenant.equals(tenant)).mapToInt(a -> a.reservations.size())
                    .sum();
        } finally {
            lock.unlock();
        }
    }

    public int queued() {
        lock.lock();
        try {
            return queue.size() + (int) agents.values().stream().filter(a -> a.offer != null).count();
        } finally {
            lock.unlock();
        }
    }

    // ---- the registry -----------------------------------------------------------------------

    private void locked(Runnable r) {
        lock.lock();
        try {
            r.run();
        } finally {
            lock.unlock();
        }
    }

    private <T> T call(java.util.function.Supplier<T> r) {
        lock.lock();
        try {
            return r.get();
        } finally {
            lock.unlock();
        }
    }

    private Agent agent(String tenant, String id) {
        return agents.computeIfAbsent(tenant + "/" + id, k -> new Agent(tenant, id));
    }

    private AgentView view(Agent a) {
        return AgentView.newBuilder().setState(a.state).setReasonCode(a.reason).setPendingTarget(a.pending)
                .setPendingReasonCode(a.pendingReason).setSinceMs(a.sinceMs).setStateVersion(a.version)
                .setMode(Mode.MODE_INBOUND).build();
    }

    private void change(Agent a, AgentState state, String reason) {
        a.state = state;
        a.reason = state == AgentState.AGENT_STATE_NOT_READY ? reason : "";
        a.sinceMs = clock.getAsLong();
        a.version++;
        send(a, WatchResponse.newBuilder().setStateChanged(StateChanged.newBuilder().setAgent(view(a))));
    }

    private void send(Agent a, WatchResponse.Builder m) {
        ServerCallStreamObserver<WatchResponse> s = agentStreams.get(a.replica);
        a.seq++;
        if (s == null || a.generation == 0) {
            return;
        }
        s.onNext(m.setLeaderEpoch(leaderEpoch).setTenantId(a.tenant).setAgentId(a.id)
                .setAttachGeneration(a.generation).setDeliverySeq(a.seq).build());
    }

    private void revoke(Agent a, RevokeReason reason) {
        String offerId = a.offer.offerId();
        a.offer = null;
        send(a, WatchResponse.newBuilder().setOfferRevoked(OfferRevoked.newBuilder().setOfferId(offerId).setReason(reason)));
    }

    /** A state the agent asks for: now when idle, pending when busy. */
    private void ask(Agent a, TargetState target, String reason) {
        boolean busy = a.state == AgentState.AGENT_STATE_RESERVED || a.state == AgentState.AGENT_STATE_ON_CALL
                || a.state == AgentState.AGENT_STATE_WRAP_UP;
        if (busy) {
            a.pending = target;
            a.pendingReason = reason == null ? "" : reason;
            return;
        }
        if (a.offer != null && target != TargetState.TARGET_STATE_READY) {
            Waiting w = a.offer.call();
            revoke(a, RevokeReason.REVOKE_REASON_STATE_CHANGED);
            queue.addFirst(w);
        }
        AgentState next = switch (target) {
            case TARGET_STATE_READY -> a.offer != null ? AgentState.AGENT_STATE_OFFERED : AgentState.AGENT_STATE_READY;
            case TARGET_STATE_LOGGED_OUT -> AgentState.AGENT_STATE_LOGGED_OUT;
            default -> AgentState.AGENT_STATE_NOT_READY;
        };
        if (next != a.state || !a.reason.equals(reason == null ? "" : reason)) {
            change(a, next, reason == null ? "" : reason);
        }
    }

    /** Offers each queued call, oldest first, to the longest-idle ready attached agent. */
    private void dispatch() {
        for (var it = queue.iterator(); it.hasNext(); ) {
            Waiting w = it.next();
            Agent best = null;
            for (Agent a : agents.values()) {
                if (a.tenant.equals(w.tenant()) && a.attached && a.state == AgentState.AGENT_STATE_READY
                        && a.offer == null && !a.rejected.contains(w.interactionId())
                        && (best == null || a.sinceMs < best.sinceMs)) {
                    best = a;
                }
            }
            if (best == null) {
                continue;
            }
            it.remove();
            OfferRec o = new OfferRec(UUID.randomUUID().toString(), w, clock.getAsLong() + offerTimeoutMs);
            best.offer = o;
            if (loseOffer.remove(best.tenant + "/" + best.id)) {
                best.seq++;
            } else {
                send(best, WatchResponse.newBuilder().setOffer(Offer.newBuilder().setOfferId(o.offerId())
                        .setInteractionId(w.interactionId()).setChannel(Channel.CHANNEL_VOICE).setQueueId(w.queueId())
                        .setExpiresAtMs(o.expiresAtMs())));
            }
            change(best, AgentState.AGENT_STATE_OFFERED, "");
        }
    }

    /** The session's agent, or ABORTED/session_superseded for an older attachment. */
    private Agent session(SessionRef s) {
        FakeSupport.checkTenant(s.getTenantId());
        Agent a = agents.get(s.getTenantId() + "/" + s.getAgentId());
        if (a == null) {
            throw FakeSupport.error(Status.NOT_FOUND, "agent_unknown");
        }
        if (s.getAttachGeneration() != a.generation || !s.getSessionId().equals(a.sessionId)) {
            throw FakeSupport.error(Status.ABORTED, "session_superseded");
        }
        return a;
    }

    /** (tenant, command_id) idempotency: a repeat gets the first result; another request: conflict. */
    @SuppressWarnings("unchecked")
    private <T> T once(String tenant, String commandId, com.google.protobuf.Message request,
            java.util.function.Supplier<T> body) {
        if (!FakeSupport.COMMAND_ID.matcher(commandId).matches()) {
            throw FakeSupport.error(Status.INVALID_ARGUMENT, "invalid_request");
        }
        String key = tenant + "/" + commandId;
        com.google.protobuf.ByteString req = request.toByteString();
        Remembered r = commands.get(key);
        if (r != null) {
            if (!r.request().equals(req)) {
                throw FakeSupport.error(Status.ALREADY_EXISTS, "conflict");
            }
            if (r.status() != null) {
                throw FakeSupport.error(r.status().toStatus(), r.code());
            }
            return (T) r.response();
        }
        try {
            T out = body.get();
            commands.put(key, new Remembered(req, out, null, null));
            return out;
        } catch (io.grpc.StatusRuntimeException e) {
            String code = e.getTrailers() == null ? null : e.getTrailers().get(parity.gateway.agents.GrpcCalls.PARITY_ERROR);
            if (!"session_superseded".equals(code)) {
                commands.put(key, new Remembered(req, null, e.getStatus().getCode(), code));
            }
            throw e;
        }
    }

    /** Copies a SetStateRequest's identity-free part, so a repeat compares equal across fields. */
    private static com.google.protobuf.Message withoutSession(com.google.protobuf.Message m) {
        com.google.protobuf.Message.Builder b = m.toBuilder();
        b.clearField(m.getDescriptorForType().findFieldByName("session"));
        return b.build();
    }

    private final class Agents extends AgentServiceGrpc.AgentServiceImplBase {

        @Override
        public void attachAgent(AttachAgentRequest r, StreamObserver<AttachAgentResponse> out) {
            FakeSupport.reply(out, () -> call(() -> {
                FakeSupport.checkTenant(r.getTenantId());
                if (!FakeSupport.UUID.matcher(r.getSessionId()).matches() || r.getAgentId().isEmpty()) {
                    throw FakeSupport.error(Status.INVALID_ARGUMENT, "invalid_request");
                }
                Agent a = agent(r.getTenantId(), r.getAgentId());
                boolean existing = r.getSessionId().equals(a.sessionId);
                if (!existing) {
                    if (a.generation > 0 && a.attached) {
                        send(a, WatchResponse.newBuilder().setDetached(Detached.newBuilder()
                                .setReason(DetachedReason.DETACHED_REASON_SUPERSEDED)));
                    }
                    a.generation++;
                    a.seq = 0;
                    a.sessionId = r.getSessionId();
                }
                a.replica = r.getReplicaId();
                a.contact = r.getAgentContact();
                a.attached = true;
                a.detachedAtMs = 0;
                if (a.state == AgentState.AGENT_STATE_LOGGED_OUT) {
                    a.state = AgentState.AGENT_STATE_NOT_READY;
                    a.reason = "login";
                    a.sinceMs = clock.getAsLong();
                    a.version++;
                }
                AttachAgentResponse.Builder b = AttachAgentResponse.newBuilder().setAttachGeneration(a.generation)
                        .setExisting(existing).setAgent(view(a)).setLeaderEpoch(leaderEpoch)
                        .addAllReservations(a.reservations.values());
                if (a.offer != null) {
                    b.setOffer(Offer.newBuilder().setOfferId(a.offer.offerId()).setInteractionId(a.offer.call().interactionId())
                            .setChannel(Channel.CHANNEL_VOICE).setQueueId(a.offer.call().queueId())
                            .setExpiresAtMs(a.offer.expiresAtMs()));
                }
                return b.build();
            }));
        }

        @Override
        public void detachAgent(DetachAgentRequest r, StreamObserver<DetachAgentResponse> out) {
            FakeSupport.reply(out, () -> call(() -> {
                FakeSupport.checkTenant(r.getSession().getTenantId());
                Agent a = agents.get(r.getSession().getTenantId() + "/" + r.getSession().getAgentId());
                if (a == null || a.generation != r.getSession().getAttachGeneration()
                        || !a.sessionId.equals(r.getSession().getSessionId()) || !a.attached) {
                    return DetachAgentResponse.newBuilder().setApplied(false).build();
                }
                a.attached = false;
                a.detachedAtMs = clock.getAsLong();
                if (a.offer != null) {
                    Waiting w = a.offer.call();
                    revoke(a, RevokeReason.REVOKE_REASON_DETACHED);
                    queue.addFirst(w);
                    change(a, AgentState.AGENT_STATE_READY, "");
                }
                if (r.getReason() == DetachReason.DETACH_REASON_LOGOUT) {
                    ask(a, TargetState.TARGET_STATE_LOGGED_OUT, "");
                }
                dispatch();
                return DetachAgentResponse.newBuilder().setApplied(true).build();
            }));
        }

        @Override
        public void setState(SetStateRequest r, StreamObserver<SetStateResponse> out) {
            FakeSupport.reply(out, () -> call(() -> {
                Agent a = session(r.getSession());
                return once(a.tenant, r.getCommandId(), withoutSession(r), () -> {
                    if (r.getTarget() == TargetState.TARGET_STATE_UNSPECIFIED) {
                        throw FakeSupport.error(Status.INVALID_ARGUMENT, "invalid_request");
                    }
                    if (r.getTarget() == TargetState.TARGET_STATE_READY && a.state == AgentState.AGENT_STATE_LOGGED_OUT) {
                        throw FakeSupport.error(Status.FAILED_PRECONDITION, "invalid_transition");
                    }
                    ask(a, r.getTarget(), r.getReasonCode());
                    dispatch();
                    return SetStateResponse.newBuilder().setAgent(view(a)).build();
                });
            }));
        }

        @Override
        public void answerOffer(AnswerOfferRequest r, StreamObserver<AnswerOfferResponse> out) {
            FakeSupport.reply(out, () -> call(() -> {
                Agent a = session(r.getSession());
                return once(a.tenant, r.getCommandId(), withoutSession(r), () -> {
                    if (a.offer == null || !a.offer.offerId().equals(r.getOfferId())) {
                        throw FakeSupport.error(Status.FAILED_PRECONDITION, "offer_gone");
                    }
                    OfferRec o = a.offer;
                    a.offer = null;
                    if (r.getDecision() != Decision.DECISION_ACCEPT) {
                        a.rejected.add(o.call().interactionId());
                        queue.addFirst(o.call());
                        change(a, AgentState.AGENT_STATE_READY, "");
                        dispatch();
                        return AnswerOfferResponse.newBuilder().setDecision(Decision.DECISION_REJECT).build();
                    }
                    Reservation res = Reservation.newBuilder().setReservationId(UUID.randomUUID().toString())
                            .setInteractionId(o.call().interactionId()).setOrigin(ReservationOrigin.RESERVATION_ORIGIN_INBOUND)
                            .setDeadlineMs(clock.getAsLong() + 30_000).build();
                    a.reservations.put(res.getReservationId(), res);
                    send(a, WatchResponse.newBuilder().setReserved(res));
                    change(a, AgentState.AGENT_STATE_RESERVED, "");
                    assigned(a, o.call(), res);
                    return AnswerOfferResponse.newBuilder().setDecision(Decision.DECISION_ACCEPT).setReservation(res).build();
                });
            }));
        }

        @Override
        public void endWrapUp(EndWrapUpRequest r, StreamObserver<EndWrapUpResponse> out) {
            FakeSupport.reply(out, () -> call(() -> {
                Agent a = session(r.getSession());
                return once(a.tenant, r.getCommandId(), withoutSession(r), () -> {
                    Reservation res = a.reservations.get(r.getReservationId());
                    if (res == null || !res.getWrapUp()) {
                        throw FakeSupport.error(Status.NOT_FOUND, "reservation_unknown");
                    }
                    a.reservations.remove(r.getReservationId());
                    wrapUpEnded(a, res);
                    if (a.reservations.isEmpty()) {
                        AgentState next = switch (a.pending) {
                            case TARGET_STATE_NOT_READY -> AgentState.AGENT_STATE_NOT_READY;
                            case TARGET_STATE_LOGGED_OUT -> AgentState.AGENT_STATE_LOGGED_OUT;
                            default -> AgentState.AGENT_STATE_READY;
                        };
                        String reason = a.pendingReason;
                        a.pending = TargetState.TARGET_STATE_UNSPECIFIED;
                        a.pendingReason = "";
                        change(a, next, reason);
                    }
                    dispatch();
                    return EndWrapUpResponse.newBuilder().setAgent(view(a)).build();
                });
            }));
        }

        @Override
        public void acknowledge(AcknowledgeRequest r, StreamObserver<AcknowledgeResponse> out) {
            FakeSupport.reply(out, AcknowledgeResponse::getDefaultInstance);
        }

        @Override
        @SuppressWarnings("ReferenceEquality") // the stream this call opened, not an equal one
        public void watch(WatchRequest r, StreamObserver<WatchResponse> out) {
            ServerCallStreamObserver<WatchResponse> s = (ServerCallStreamObserver<WatchResponse>) out;
            locked(() -> {
                ServerCallStreamObserver<WatchResponse> old = agentStreams.put(r.getReplicaId(), s);
                if (old != null) {
                    old.onError(FakeSupport.error(Status.ABORTED, "stream_superseded"));
                }
                s.onNext(WatchResponse.newBuilder().setLeaderEpoch(leaderEpoch)
                        .setHeartbeat(Heartbeat.newBuilder().setAtMs(clock.getAsLong())).build());
            });
            s.setOnCancelHandler(() -> locked(() -> {
                if (agentStreams.get(r.getReplicaId()) == s) {
                    agentStreams.remove(r.getReplicaId());
                    for (Agent a : agents.values()) {
                        if (a.attached && a.replica.equals(r.getReplicaId())) {
                            a.attached = false;
                            a.detachedAtMs = clock.getAsLong();
                        }
                    }
                }
            }));
        }
    }

    // ---- the IE half (routing.v1, minimal) --------------------------------------------------

    private void assigned(Agent a, Waiting w, Reservation res) {
        var m = parity.contracts.routing.v1.WatchResponse.newBuilder().setLeaderEpoch(leaderEpoch)
                .setDeliverySeq(ieSeq.incrementAndGet())
                .setAssigned(Assigned.newBuilder().setTarget(EventTarget.newBuilder().setTenantId(a.tenant)
                                .setInteractionId(w.interactionId()).setOwnerEpoch(w.ownerEpoch()))
                        .setReservationId(res.getReservationId()).setEntryId(w.entryId()).setAgentId(a.id)
                        .setAgentContact(a.contact).setDeadlineMs(res.getDeadlineMs()))
                .build();
        ieStreams.forEach(s -> s.onNext(m));
    }

    private void wrapUpEnded(Agent a, Reservation res) {
        var m = parity.contracts.routing.v1.WatchResponse.newBuilder().setLeaderEpoch(leaderEpoch)
                .setDeliverySeq(ieSeq.incrementAndGet())
                .setWrapUpEnded(WrapUpEnded.newBuilder().setTarget(EventTarget.newBuilder().setTenantId(a.tenant)
                                .setInteractionId(res.getInteractionId()))
                        .setReservationId(res.getReservationId()).setAgentId(a.id).setEndedAtMs(clock.getAsLong()))
                .build();
        ieStreams.forEach(s -> s.onNext(m));
    }

    private final class Routing extends RoutingServiceGrpc.RoutingServiceImplBase {

        @Override
        public void enqueue(EnqueueRequest r, StreamObserver<EnqueueResponse> out) {
            FakeSupport.reply(out, () -> call(() -> {
                FakeSupport.checkTenant(r.getOwner().getTenantId());
                String entry = UUID.randomUUID().toString();
                queue.addLast(new Waiting(r.getOwner().getTenantId(), r.getOwner().getInteractionId(),
                        r.getTarget().getQueueId(), r.getOrderingAtMs(), entry, r.getOwner().getOwnerEpoch()));
                dispatch();
                return EnqueueResponse.newBuilder().setEntry(QueueEntry.newBuilder().setEntryId(entry)).build();
            }));
        }

        @Override
        public void dequeue(DequeueRequest r, StreamObserver<DequeueResponse> out) {
            FakeSupport.reply(out, () -> call(() -> {
                FakeSupport.checkTenant(r.getOwner().getTenantId());
                queue.removeIf(w -> w.entryId().equals(r.getEntryId()));
                for (Agent a : agents.values()) {
                    if (a.offer != null && a.offer.call().entryId().equals(r.getEntryId())) {
                        revoke(a, RevokeReason.REVOKE_REASON_CALLER_GONE);
                        change(a, AgentState.AGENT_STATE_READY, "");
                    }
                }
                return DequeueResponse.getDefaultInstance();
            }));
        }

        @Override
        public void reportOutcome(ReportOutcomeRequest r, StreamObserver<ReportOutcomeResponse> out) {
            FakeSupport.reply(out, () -> call(() -> {
                FakeSupport.checkTenant(r.getOwner().getTenantId());
                for (Agent a : agents.values()) {
                    Reservation res = a.reservations.get(r.getReservationId());
                    if (res != null) {
                        if (r.getOutcome() == Outcome.OUTCOME_CONNECTED) {
                            change(a, AgentState.AGENT_STATE_ON_CALL, "");
                        } else {
                            a.reservations.remove(r.getReservationId());
                            change(a, AgentState.AGENT_STATE_READY, "");
                        }
                    }
                }
                dispatch();
                return ReportOutcomeResponse.getDefaultInstance();
            }));
        }

        @Override
        public void callEnded(CallEndedRequest r, StreamObserver<CallEndedResponse> out) {
            FakeSupport.reply(out, () -> {
                FakeSupport.checkTenant(r.getOwner().getTenantId());
                FakeRoutingEngine.this.callEnded(r.getOwner().getTenantId(), r.getReservationId());
                return CallEndedResponse.getDefaultInstance();
            });
        }

        @Override
        public void watch(parity.contracts.routing.v1.WatchRequest r,
                StreamObserver<parity.contracts.routing.v1.WatchResponse> out) {
            var s = (ServerCallStreamObserver<parity.contracts.routing.v1.WatchResponse>) out;
            locked(() -> {
                ieStreams.add(s);
            });
            s.setOnCancelHandler(() -> locked(() -> {
                ieStreams.remove(s);
            }));
        }
    }

    @Override
    public void close() {
        if (ticker != null) {
            ticker.shutdownNow();
        }
        if (server != null) {
            server.shutdownNow();
        }
    }
}
