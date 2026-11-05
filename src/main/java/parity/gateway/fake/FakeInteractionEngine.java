package parity.gateway.fake;

import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;
import parity.contracts.interaction.v1.AgentCallView;
import parity.contracts.interaction.v1.CallState;
import parity.contracts.interaction.v1.ExecuteRequest;
import parity.contracts.interaction.v1.ExecuteResponse;
import parity.contracts.interaction.v1.GetRequest;
import parity.contracts.interaction.v1.GetResponse;
import parity.contracts.interaction.v1.InteractionControlServiceGrpc;
import parity.contracts.interaction.v1.Outcome;
import parity.contracts.interaction.v1.Party;
import parity.contracts.interaction.v1.PartyRole;
import parity.contracts.interaction.v1.RecordingState;

/**
 * A fake interaction-engine serving {@code parity.interaction.v1.InteractionControlService} with
 * the contract's rules: the authorization rule (the current reservation's agent, or an agent with
 * a live leg; else {@code not_your_call}), {@code (tenant, command_id)} idempotency with
 * {@code conflict}, {@code call_unmanaged}, blocked destinations, {@code owner_moving}.
 */
public final class FakeInteractionEngine implements AutoCloseable {

    private static final class Leg {
        final String legId;
        final PartyRole role;
        final String agentId;
        boolean live = true;
        boolean muted;

        Leg(String legId, PartyRole role, String agentId) {
            this.legId = legId;
            this.role = role;
            this.agentId = agentId;
        }
    }

    private static final class Call {
        final String tenant;
        final String interactionId;
        String reservationId;
        String reservationAgent;
        final List<Leg> legs = new ArrayList<>();
        boolean held;
        boolean consulting;
        boolean managed = true;
        boolean ended;
        long ownerEpoch = 1;
        long seq;

        Call(String tenant, String interactionId) {
            this.tenant = tenant;
            this.interactionId = interactionId;
        }
    }

    private record Done(ByteString action, AgentCallView view) {}

    private final ReentrantLock lock = new ReentrantLock();
    private final Map<String, Call> calls = new HashMap<>();
    private final Map<String, Done> commands = new HashMap<>();
    private final Set<String> blocked = new HashSet<>();
    private volatile boolean ownerMoving;
    private Server server;

    public FakeInteractionEngine start(int port) throws IOException {
        server = NettyServerBuilder.forPort(port)
                .addService(ServerInterceptors.intercept(new Service(), FakeSupport.callers())).build().start();
        return this;
    }

    public int port() {
        return server.getPort();
    }

    /** A connected call: the customer's leg and the reserved agent's leg. */
    public void seedCall(String tenant, String interactionId, String reservationId, String agentId, String customerLeg,
            String agentLeg, boolean managed, long ownerEpoch, long seq) {
        lock.lock();
        try {
            Call c = new Call(tenant, interactionId);
            c.reservationId = reservationId;
            c.reservationAgent = agentId;
            c.legs.add(new Leg(customerLeg, PartyRole.PARTY_ROLE_CUSTOMER, ""));
            c.legs.add(new Leg(agentLeg, PartyRole.PARTY_ROLE_AGENT, agentId));
            c.managed = managed;
            c.ownerEpoch = ownerEpoch;
            c.seq = seq;
            calls.put(interactionId, c);
        } finally {
            lock.unlock();
        }
    }

    public void block(String number) {
        blocked.add(number);
    }

    public void ownerMoving(boolean moving) {
        ownerMoving = moving;
    }

    public void managed(String interactionId, boolean managed) {
        lock.lock();
        try {
            calls.get(interactionId).managed = managed;
        } finally {
            lock.unlock();
        }
    }

    private AgentCallView view(Call c, String agentId) {
        AgentCallView.Builder b = AgentCallView.newBuilder().setInteractionId(c.interactionId)
                .setState(c.ended ? CallState.CALL_STATE_ENDED : CallState.CALL_STATE_CONNECTED)
                .setHeld(c.held).setConsulting(c.consulting).setRecording(RecordingState.RECORDING_STATE_ON)
                .setManaged(c.managed).setOwnerEpoch(c.ownerEpoch).setSeq(c.seq)
                .setReservationId(c.reservationId == null ? "" : c.reservationId);
        for (Leg l : c.legs) {
            b.addParties(Party.newBuilder().setRole(l.role).setAgentId(l.agentId).setLegId(l.legId).setLive(l.live));
            if (l.live && agentId.equals(l.agentId) && l.muted) {
                b.setMuted(true);
            }
        }
        return b.build();
    }

    private Leg liveLeg(Call c, String agentId) {
        for (Leg l : c.legs) {
            if (l.live && agentId.equals(l.agentId)) {
                return l;
            }
        }
        return null;
    }

    /** The checks in the contract's order: tenant, request, owner, the call, the authorization. */
    private Call authorized(String tenant, String interactionId, String agentId) {
        FakeSupport.checkTenant(tenant);
        if (!FakeSupport.UUID.matcher(interactionId).matches() || agentId.isEmpty()) {
            throw FakeSupport.error(Status.INVALID_ARGUMENT, "invalid_request");
        }
        if (ownerMoving) {
            throw FakeSupport.error(Status.UNAVAILABLE, "owner_moving");
        }
        Call c = calls.get(interactionId);
        if (c == null || !c.tenant.equals(tenant)) {
            throw FakeSupport.error(Status.NOT_FOUND, "not_found");
        }
        boolean holdsReservation = c.reservationId != null && !c.reservationId.isEmpty() && agentId.equals(c.reservationAgent);
        if (!holdsReservation && liveLeg(c, agentId) == null) {
            throw FakeSupport.error(Status.PERMISSION_DENIED, "not_your_call");
        }
        return c;
    }

    private static StatusIfNot invalidState() {
        return new StatusIfNot();
    }

    /** A FAILED_PRECONDITION/invalid_state thrower, for readability. */
    private static final class StatusIfNot {
        void unless(boolean ok) {
            if (!ok) {
                throw FakeSupport.error(Status.FAILED_PRECONDITION, "invalid_state");
            }
        }
    }

    private void apply(Call c, ExecuteRequest r) {
        Leg mine = liveLeg(c, r.getAgentId());
        switch (r.getActionCase()) {
            case HOLD -> {
                invalidState().unless(!c.held);
                c.held = true;
            }
            case RESUME -> {
                invalidState().unless(c.held && !c.consulting);
                c.held = false;
            }
            case MUTE, UNMUTE -> {
                invalidState().unless(mine != null);
                mine.muted = r.getActionCase() == ExecuteRequest.ActionCase.MUTE;
            }
            case BLIND_TRANSFER -> {
                String number = r.getBlindTransfer().getTarget().getNumber();
                if (!number.isEmpty() && blocked.contains(number)) {
                    throw FakeSupport.error(Status.FAILED_PRECONDITION, "destination_not_allowed");
                }
                invalidState().unless(mine != null);
                mine.live = false;
                c.reservationId = "";
                c.reservationAgent = "";
                c.held = false;
            }
            case CONSULT -> {
                invalidState().unless(!c.consulting && mine != null);
                String target = r.getConsult().getTarget().getAgentId();
                if (!r.getConsult().getTarget().getNumber().isEmpty() && blocked.contains(r.getConsult().getTarget().getNumber())) {
                    throw FakeSupport.error(Status.FAILED_PRECONDITION, "destination_not_allowed");
                }
                c.legs.add(new Leg(UUID.randomUUID().toString(), target.isEmpty() ? PartyRole.PARTY_ROLE_EXTERNAL
                        : PartyRole.PARTY_ROLE_AGENT, target));
                c.held = true;
                c.consulting = true;
            }
            case COMPLETE_TRANSFER -> {
                invalidState().unless(c.consulting && mine != null);
                mine.live = false;
                c.consulting = false;
                c.held = false;
                c.reservationId = "";
                c.reservationAgent = "";
            }
            case MERGE -> {
                invalidState().unless(c.consulting);
                c.consulting = false;
                c.held = false;
            }
            case CANCEL_CONSULT -> {
                invalidState().unless(c.consulting);
                Leg last = c.legs.get(c.legs.size() - 1);
                last.live = false;
                c.consulting = false;
            }
            case REMOVE_PARTY -> {
                boolean found = false;
                for (Leg l : c.legs) {
                    if (l.legId.equals(r.getRemoveParty().getLegId()) && l.live) {
                        l.live = false;
                        found = true;
                    }
                }
                invalidState().unless(found);
            }
            case HANGUP -> {
                invalidState().unless(mine != null);
                mine.live = false;
                if (c.legs.stream().noneMatch(l -> l.live && l.role == PartyRole.PARTY_ROLE_AGENT)) {
                    c.ended = true;
                }
            }
            case ACTION_NOT_SET -> throw FakeSupport.error(Status.INVALID_ARGUMENT, "invalid_request");
        }
        c.seq++;
    }

    private final class Service extends InteractionControlServiceGrpc.InteractionControlServiceImplBase {

        @Override
        public void execute(ExecuteRequest r, StreamObserver<ExecuteResponse> out) {
            FakeSupport.reply(out, () -> {
                lock.lock();
                try {
                    if (!FakeSupport.COMMAND_ID.matcher(r.getCommandId()).matches()) {
                        FakeSupport.checkTenant(r.getTenantId());
                        throw FakeSupport.error(Status.INVALID_ARGUMENT, "invalid_request");
                    }
                    Call c = authorized(r.getTenantId(), r.getInteractionId(), r.getAgentId());
                    String key = r.getTenantId() + "/" + r.getCommandId();
                    ByteString action = r.toBuilder().clearReservationId().build().toByteString();
                    Done done = commands.get(key);
                    if (done != null) {
                        if (!done.action().equals(action)) {
                            throw FakeSupport.error(Status.ALREADY_EXISTS, "conflict");
                        }
                        return ExecuteResponse.newBuilder().setOutcome(Outcome.OUTCOME_ALREADY_DONE).setCall(done.view()).build();
                    }
                    if (!c.managed) {
                        throw FakeSupport.error(Status.FAILED_PRECONDITION, "call_unmanaged");
                    }
                    apply(c, r);
                    AgentCallView v = view(c, r.getAgentId());
                    commands.put(key, new Done(action, v));
                    return ExecuteResponse.newBuilder().setOutcome(Outcome.OUTCOME_DONE).setCall(v).build();
                } finally {
                    lock.unlock();
                }
            });
        }

        @Override
        public void get(GetRequest r, StreamObserver<GetResponse> out) {
            FakeSupport.reply(out, () -> {
                lock.lock();
                try {
                    Call c = authorized(r.getTenantId(), r.getInteractionId(), r.getAgentId());
                    return GetResponse.newBuilder().setCall(view(c, r.getAgentId())).build();
                } finally {
                    lock.unlock();
                }
            });
        }
    }

    @Override
    public void close() {
        if (server != null) {
            server.shutdownNow();
        }
    }
}
