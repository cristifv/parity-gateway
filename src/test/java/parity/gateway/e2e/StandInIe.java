package parity.gateway.e2e;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BooleanSupplier;
import parity.contracts.routing.v1.Assigned;
import parity.contracts.routing.v1.CallEndedRequest;
import parity.contracts.routing.v1.EnqueueRequest;
import parity.contracts.routing.v1.InteractionOwner;
import parity.contracts.routing.v1.Outcome;
import parity.contracts.routing.v1.ReportOutcomeRequest;
import parity.contracts.routing.v1.RoutingServiceGrpc;
import parity.contracts.routing.v1.Target;
import parity.contracts.telephony.v1.AcknowledgeRequest;
import parity.contracts.telephony.v1.AcknowledgeResponse;
import parity.contracts.telephony.v1.AddLegRequest;
import parity.contracts.telephony.v1.AnswerLegRequest;
import parity.contracts.telephony.v1.Arrived;
import parity.contracts.telephony.v1.ClaimSessionRequest;
import parity.contracts.telephony.v1.CodecPolicy;
import parity.contracts.telephony.v1.EndCause;
import parity.contracts.telephony.v1.EndSessionRequest;
import parity.contracts.telephony.v1.Endpoint;
import parity.contracts.telephony.v1.LegRole;
import parity.contracts.telephony.v1.LegState;
import parity.contracts.telephony.v1.LegUpdate;
import parity.contracts.telephony.v1.SessionOwner;
import parity.contracts.telephony.v1.TelephonyServiceGrpc;
import parity.contracts.telephony.v1.WatchRequest;
import parity.contracts.telephony.v1.WatchResponse;

/**
 * A stand-in for interaction-engine (step 5a; the real IE comes in 5b): one replica of bin-a for
 * tenant acme. It registers in etcd, watches TC, claims an arrival (owner record create-if-absent,
 * an existing one accepted only when every field is ours; released only while it still holds our
 * exact value), answers the customer, and either enqueues to the fake RE and adds the agent's leg
 * when RE assigns it (the real flow), or adds the leg with no reservation (the negative case).
 */
final class StandInIe implements AutoCloseable {

    static final String TENANT = "acme";
    static final String BIN = "bin-a";
    static final String REPLICA = "ie-standin-1";
    static final String NODE = "media-1";

    private static final SecureRandom RANDOM = new SecureRandom();

    private final ManagedChannel tcChannel;
    private final ManagedChannel reChannel;
    private final TelephonyServiceGrpc.TelephonyServiceBlockingStub tc;
    private final RoutingServiceGrpc.RoutingServiceBlockingStub re;
    private final Etcd etcd;
    private final Map<String, Arrived> arrivals = new ConcurrentHashMap<>();
    private final Map<String, List<LegUpdate>> legs = new ConcurrentHashMap<>();
    private final Map<String, Long> lastSeq = new ConcurrentHashMap<>();
    private final Map<String, Long> epochs = new ConcurrentHashMap<>();
    private final Map<String, Assigned> assigned = new ConcurrentHashMap<>();
    private final List<String> timeline = Collections.synchronizedList(new ArrayList<>());
    private final long t0 = System.currentTimeMillis();
    private volatile String incarnation = "";

    StandInIe(int tcPort, int rePort, Etcd etcd) {
        this.etcd = etcd;
        Metadata md = new Metadata();
        md.put(Metadata.Key.of("x-parity-test-peer", Metadata.ASCII_STRING_MARSHALLER), "parity-interaction-engine." + BIN);
        md.put(Metadata.Key.of("tenant-id", Metadata.ASCII_STRING_MARSHALLER), TENANT);
        tcChannel = ManagedChannelBuilder.forAddress("127.0.0.1", tcPort).usePlaintext()
                .intercept(MetadataUtils.newAttachHeadersInterceptor(md)).build();
        reChannel = ManagedChannelBuilder.forAddress("127.0.0.1", rePort).usePlaintext()
                .intercept(MetadataUtils.newAttachHeadersInterceptor(md)).build();
        tc = TelephonyServiceGrpc.newBlockingStub(tcChannel);
        re = RoutingServiceGrpc.newBlockingStub(reChannel);
    }

    void start() throws IOException, InterruptedException {
        etcd.put("ie/replicas/" + BIN + "/" + REPLICA, "{}");
        TelephonyServiceGrpc.newStub(tcChannel).watch(WatchRequest.newBuilder().setReplicaId(REPLICA).setBin(BIN).build(),
                new StreamObserver<>() {
                    @Override
                    public void onNext(WatchResponse r) {
                        onTc(r);
                    }

                    @Override
                    public void onError(Throwable t) {
                        note("TC watch ended: " + io.grpc.Status.fromThrowable(t).getCode());
                    }

                    @Override
                    public void onCompleted() {
                        note("TC watch completed");
                    }
                });
        RoutingServiceGrpc.newStub(reChannel).watch(parity.contracts.routing.v1.WatchRequest.newBuilder()
                .setReplicaId(REPLICA).setBin(BIN).build(), new StreamObserver<>() {
                    @Override
                    public void onNext(parity.contracts.routing.v1.WatchResponse r) {
                        if (r.hasAssigned()) {
                            Assigned a = r.getAssigned();
                            assigned.put(a.getTarget().getInteractionId(), a);
                            note("RE Assigned " + a.getTarget().getInteractionId() + " agent " + a.getAgentId()
                                    + " contact " + a.getAgentContact());
                        }
                    }

                    @Override
                    public void onError(Throwable t) {
                        note("RE watch ended: " + io.grpc.Status.fromThrowable(t).getCode());
                    }

                    @Override
                    public void onCompleted() {
                        note("RE watch completed");
                    }
                });
    }

    private void onTc(WatchResponse r) {
        incarnation = r.getIncarnation();
        if (!r.hasSession()) {
            return;
        }
        String iid = r.getSession().getInteractionId();
        Long before = lastSeq.get(iid);
        lastSeq.merge(iid, r.getDeliverySeq(), Math::max);
        // Acknowledged at once (as IE does once it has processed an event), so TC stops replaying it.
        TelephonyServiceGrpc.newStub(tcChannel).acknowledge(AcknowledgeRequest.newBuilder().addAcks(owner(iid)).build(),
                new StreamObserver<AcknowledgeResponse>() {
                    @Override
                    public void onNext(AcknowledgeResponse a) {
                        if (a.getIgnored() > 0) {
                            note("TC ignored an ack for " + iid);
                        }
                    }

                    @Override
                    public void onError(Throwable t) {
                        note("Acknowledge failed: " + io.grpc.Status.fromThrowable(t).getCode());
                    }

                    @Override
                    public void onCompleted() {
                        // acknowledged
                    }
                });
        if (before != null && r.getDeliverySeq() <= before) {
            return; // a replay of what we already have
        }
        if (r.hasArrived()) {
            arrivals.put(iid, r.getArrived());
            note("TC Arrived " + iid + " leg " + r.getArrived().getLegId() + " attempt " + r.getArrived().getAttempt());
        } else if (r.hasLeg()) {
            LegUpdate u = r.getLeg();
            legs.computeIfAbsent(u.getLegId(), k -> Collections.synchronizedList(new ArrayList<>())).add(u);
            note("TC LegUpdate " + iid + " leg " + u.getLegId() + " " + u.getRole() + " " + u.getState()
                    + (u.getState() == LegState.LEG_STATE_ENDED ? " " + u.getCause() + " sip " + u.getSipStatus() : ""));
        } else if (r.hasSessionEnded()) {
            note("TC SessionEnded " + iid);
        }
    }

    Arrived awaitArrival(String iid, Duration within) throws InterruptedException {
        await(() -> arrivals.containsKey(iid), within, "no Arrived for the call");
        return arrivals.get(iid);
    }

    Assigned awaitAssigned(String iid, Duration within) throws InterruptedException {
        await(() -> assigned.containsKey(iid), within, "RE never assigned the call");
        return assigned.get(iid);
    }

    /** Waits for a leg's state; false when the time runs out. */
    boolean awaitLeg(String legId, LegState state, Duration within) throws InterruptedException {
        long until = System.nanoTime() + within.toNanos();
        while (System.nanoTime() < until) {
            if (legStates(legId).contains(state)) {
                return true;
            }
            Thread.sleep(50);
        }
        return false;
    }

    List<LegState> legStates(String legId) {
        List<LegUpdate> l = legs.getOrDefault(legId, List.of());
        synchronized (l) {
            return l.stream().map(LegUpdate::getState).toList();
        }
    }

    LegUpdate lastLeg(String legId) {
        List<LegUpdate> l = legs.getOrDefault(legId, List.of());
        synchronized (l) {
            return l.isEmpty() ? null : l.get(l.size() - 1);
        }
    }

    /** The owner record (create-if-absent, ours only if every field matches), then ClaimSession. */
    long claim(String iid) throws IOException, InterruptedException {
        long epoch = etcd.createIfAbsent("ie/owner/" + iid, ownerValue());
        if (epoch < 1) {
            throw new IllegalStateException("the owner record exists and is not ours");
        }
        epochs.put(iid, epoch);
        var unused = tc.claimSession(ClaimSessionRequest.newBuilder().setOwner(owner(iid)).build());
        note("claimed " + iid + " epoch " + epoch);
        return epoch;
    }

    void answer(String iid, String legId) {
        var unused = tc.answerLeg(AnswerLegRequest.newBuilder().setOwner(owner(iid)).setCommandId(commandId())
                .setLegId(legId).build());
        note("AnswerLeg " + legId);
    }

    void enqueue(String iid, String queueId) {
        long now = System.currentTimeMillis();
        var unused = re.enqueue(EnqueueRequest.newBuilder().setOwner(reOwner(iid)).setEnqueueSeq(1)
                .setTarget(Target.newBuilder().setQueueId(queueId)).setFirstQueuedAtMs(now).setOrderingAtMs(now)
                .setChannel(parity.contracts.routing.v1.Channel.CHANNEL_VOICE).build());
        note("Enqueue " + iid + " queue " + queueId);
    }

    /** AddLeg toward the agent's contact (G.711 only, WebRTC); returns the new leg id (a UUIDv7). */
    String addAgentLeg(String iid, String contact, int ringTimeoutS) {
        String legId = uuid7();
        var unused = tc.addLeg(AddLegRequest.newBuilder().setOwner(owner(iid)).setCommandId(commandId()).setLegId(legId)
                .setRole(LegRole.LEG_ROLE_AGENT).setAgentContact(contact).setRingTimeoutS(ringTimeoutS)
                .setEndpoint(Endpoint.ENDPOINT_WEBRTC).setCodecPolicy(CodecPolicy.CODEC_POLICY_G711_ONLY).build());
        note("AddLeg " + legId + " agent " + contact + " ring " + ringTimeoutS + " s");
        return legId;
    }

    void connected(String iid, String reservationId) {
        var unused = re.reportOutcome(ReportOutcomeRequest.newBuilder().setOwner(reOwner(iid))
                .setReservationId(reservationId).setOutcome(Outcome.OUTCOME_CONNECTED).build());
        note("ReportOutcome CONNECTED " + reservationId);
    }

    void callEnded(String iid, String agentId, String reservationId) {
        var unused = re.callEnded(CallEndedRequest.newBuilder().setOwner(reOwner(iid)).setAgentId(agentId)
                .setReservationId(reservationId).setEndedAtMs(System.currentTimeMillis()).build());
        note("CallEnded " + reservationId);
    }

    void endSession(String iid) {
        var unused = tc.endSession(EndSessionRequest.newBuilder().setOwner(owner(iid)).setCommandId(commandId())
                .setCause(EndCause.END_CAUSE_NORMAL).build());
        note("EndSession " + iid);
    }

    /** Deletes the owner record only while it still holds exactly what we wrote. */
    boolean release(String iid) throws IOException, InterruptedException {
        boolean deleted = etcd.deleteIfValue("ie/owner/" + iid, ownerValue());
        note("released " + iid + " (deleted " + deleted + ")");
        return deleted;
    }

    List<String> timeline() {
        synchronized (timeline) {
            return List.copyOf(timeline);
        }
    }

    private SessionOwner owner(String iid) {
        return SessionOwner.newBuilder().setTenantId(TENANT).setInteractionId(iid).setOwnerEpoch(epochs.getOrDefault(iid, 0L))
                .setReplicaId(REPLICA).setAckedDeliverySeq(lastSeq.getOrDefault(iid, 0L)).setAckedIncarnation(incarnation).build();
    }

    private InteractionOwner reOwner(String iid) {
        return InteractionOwner.newBuilder().setTenantId(TENANT).setInteractionId(iid)
                .setOwnerEpoch(epochs.getOrDefault(iid, 0L)).setReplicaId(REPLICA).build();
    }

    private static String ownerValue() {
        return "{\"tenant_id\":\"" + TENANT + "\",\"bin\":\"" + BIN + "\",\"replica_id\":\"" + REPLICA + "\",\"node_id\":\""
                + NODE + "\"}";
    }

    private static String commandId() {
        return REPLICA + ":" + UUID.randomUUID();
    }

    static String uuid7() {
        long ms = System.currentTimeMillis();
        long msb = (ms << 16) | 0x7000L | (RANDOM.nextInt() & 0x0fffL);
        long lsb = (RANDOM.nextLong() & 0x3fffffffffffffffL) | 0x8000000000000000L;
        return new UUID(msb, lsb).toString();
    }

    private void note(String s) {
        timeline.add(String.format(Locale.ROOT, "%8.3f %s", (System.currentTimeMillis() - t0) / 1000.0, s));
    }

    static void await(BooleanSupplier what, Duration within, String failure) throws InterruptedException {
        long until = System.nanoTime() + within.toNanos();
        while (!what.getAsBoolean()) {
            if (System.nanoTime() > until) {
                throw new AssertionError(failure);
            }
            Thread.sleep(50);
        }
    }

    @Override
    public void close() {
        tcChannel.shutdownNow();
        reChannel.shutdownNow();
    }
}
