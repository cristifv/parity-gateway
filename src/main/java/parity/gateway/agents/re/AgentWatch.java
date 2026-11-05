package parity.gateway.agents.re;

import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import parity.contracts.agents.v1.Ack;
import parity.contracts.agents.v1.AcknowledgeRequest;
import parity.contracts.agents.v1.AgentServiceGrpc;
import parity.contracts.agents.v1.WatchRequest;
import parity.contracts.agents.v1.WatchResponse;
import parity.gateway.agents.GrpcCalls;
import parity.gateway.agents.session.SessionRegistry;

/**
 * This replica's one {@code Watch} stream to RE: messages go to the sessions, acknowledgements go
 * back batched (every 50 ms or 500). The stream is dead after 15 s without a message (3 missed
 * heartbeats) and reconnects with backoff 100 ms → 5 s, full jitter; after a reconnect or a
 * higher leader epoch every live session re-attaches. Messages of an older leader are dropped.
 */
public final class AgentWatch implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(AgentWatch.class);
    private static final long DEAD_NANOS = Duration.ofSeconds(15).toNanos();

    private final ReClient re;
    private final SessionRegistry sessions;
    private final String replicaId;
    private final String bin;
    private final ScheduledExecutorService timer =
            Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().name("re-watch").daemon(true).factory());
    private final ConcurrentLinkedQueue<Ack> acks = new ConcurrentLinkedQueue<>();
    private final AtomicReference<ClientCallStreamObserver<WatchRequest>> call = new AtomicReference<>();
    private volatile long lastMessage = System.nanoTime();
    private volatile long leaderEpoch;
    private volatile boolean everConnected;
    private volatile boolean closed;
    private long backoffMs = 100;

    public AgentWatch(ReClient re, SessionRegistry sessions, String replicaId, String bin) {
        this.re = re;
        this.sessions = sessions;
        this.replicaId = replicaId;
        this.bin = bin;
        sessions.acker(this::ack);
    }

    public void start() {
        connect();
        var unused = timer.scheduleAtFixedRate(this::flushAcks, 50, 50, TimeUnit.MILLISECONDS);
        var unused2 = timer.scheduleAtFixedRate(this::watchdog, 1, 1, TimeUnit.SECONDS);
    }

    public boolean up() {
        return call.get() != null && System.nanoTime() - lastMessage < DEAD_NANOS;
    }

    private void connect() {
        if (closed) {
            return;
        }
        lastMessage = System.nanoTime();
        AgentServiceGrpc.newStub(GrpcCalls.with(re.channel(), null, re.peer(), re.testPeer()))
                .watch(WatchRequest.newBuilder().setReplicaId(replicaId).setBin(bin).build(), new Observer());
    }

    private final class Observer implements ClientResponseObserver<WatchRequest, WatchResponse> {
        private boolean first = true;

        @Override
        public void beforeStart(ClientCallStreamObserver<WatchRequest> requestStream) {
            call.set(requestStream);
        }

        @Override
        public void onNext(WatchResponse m) {
            lastMessage = System.nanoTime();
            if (m.getLeaderEpoch() < leaderEpoch) {
                return;
            }
            boolean newLeader = leaderEpoch != 0 && m.getLeaderEpoch() > leaderEpoch;
            leaderEpoch = m.getLeaderEpoch();
            if (first) {
                first = false;
                backoffMs = 100;
                if (everConnected || newLeader) {
                    log.atInfo().addKeyValue("event", "routing.stream_restored").addKeyValue("bin", bin)
                            .addKeyValue("leader_epoch", leaderEpoch).addKeyValue("new_leader", newLeader)
                            .log("RE stream back: re-attaching sessions");
                    sessions.reattachAll();
                }
                everConnected = true;
            } else if (newLeader) {
                log.atInfo().addKeyValue("event", "routing.leader_changed").addKeyValue("bin", bin)
                        .addKeyValue("leader_epoch", leaderEpoch).log("new RE leader: re-attaching sessions");
                sessions.reattachAll();
            }
            if (m.getEventCase() != WatchResponse.EventCase.HEARTBEAT) {
                sessions.onRe(m);
            }
        }

        @Override
        public void onError(Throwable t) {
            retry();
        }

        @Override
        public void onCompleted() {
            retry();
        }
    }

    private void retry() {
        call.set(null);
        if (closed) {
            return;
        }
        long pause = ThreadLocalRandom.current().nextLong(backoffMs + 1);
        backoffMs = Math.min(5000, backoffMs * 2);
        log.atWarn().addKeyValue("event", "routing.stream_lost").addKeyValue("bin", bin).addKeyValue("retry_ms", pause)
                .log("RE stream lost; reconnecting");
        var unused = timer.schedule(this::connect, pause, TimeUnit.MILLISECONDS);
    }

    private void watchdog() {
        ClientCallStreamObserver<WatchRequest> c = call.get();
        if (c != null && System.nanoTime() - lastMessage > DEAD_NANOS) {
            log.atWarn().addKeyValue("event", "routing.stream_silent").addKeyValue("bin", bin)
                    .log("RE stream silent for 15 s; cancelling it");
            c.cancel("no heartbeat", null);
        }
    }

    private void ack(WatchResponse m) {
        if (m.getDeliverySeq() == 0) {
            return;
        }
        acks.add(Ack.newBuilder().setTenantId(m.getTenantId()).setAgentId(m.getAgentId())
                .setAttachGeneration(m.getAttachGeneration()).setDeliverySeq(m.getDeliverySeq()).build());
    }

    private void flushAcks() {
        while (!acks.isEmpty()) {
            List<Ack> batch = new ArrayList<>();
            Ack a;
            while (batch.size() < 500 && (a = acks.poll()) != null) {
                batch.add(a);
            }
            try {
                AgentServiceGrpc.newBlockingStub(GrpcCalls.with(re.channel(), null, re.peer(), re.testPeer()))
                        .withDeadlineAfter(300, TimeUnit.MILLISECONDS)
                        .acknowledge(AcknowledgeRequest.newBuilder().setReplicaId(replicaId).addAllAcks(batch).build());
            } catch (io.grpc.StatusRuntimeException e) {
                // RE redelivers what isn't acknowledged; the sessions drop repeats by delivery_seq.
                log.atWarn().addKeyValue("event", "routing.acks_failed").addKeyValue("bin", bin)
                        .addKeyValue("error_code", e.getStatus().getCode().name().toLowerCase(java.util.Locale.ROOT))
                        .addKeyValue("acks", batch.size()).log("acknowledgements not delivered; RE redelivers");
                return;
            }
        }
    }

    @Override
    public void close() {
        closed = true;
        ClientCallStreamObserver<WatchRequest> c = call.get();
        if (c != null) {
            c.cancel("closing", null);
        }
        timer.shutdownNow();
    }
}
