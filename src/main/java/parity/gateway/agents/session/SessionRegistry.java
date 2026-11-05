package parity.gateway.agents.session;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.socket.WebSocketSession;
import parity.contracts.agents.v1.WatchResponse;
import parity.gateway.agents.auth.Identity;
import parity.gateway.lifecycle.Drainable;

/**
 * This replica's sessions: one per agent (a newer one supersedes, 4009), an interaction → session
 * index for IE's events, admission (a per-replica rate and a per-tenant cap), and the drain:
 * every desktop is told to reconnect after a random 0–{@code drainSpread} delay, the rest closed
 * with 1001 near the timeout.
 */
public final class SessionRegistry implements Drainable {

    private static final Logger log = LoggerFactory.getLogger(SessionRegistry.class);

    private final ConcurrentHashMap<String, AgentSession> byAgent = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Set<AgentSession>> byInteraction = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicInteger> perTenant = new ConcurrentHashMap<>();
    private final TokenBucket admission;
    private final Deps deps;
    private volatile Consumer<WatchResponse> acker = m -> {};
    private volatile boolean draining;

    public SessionRegistry(Deps deps) {
        this.deps = deps;
        int rate = deps.agents().admissionPerSecond();
        this.admission = new TokenBucket(rate, rate);
    }

    static String key(String tenantId, String agentId) {
        return tenantId + "/" + agentId;
    }

    public void acker(Consumer<WatchResponse> acker) {
        this.acker = acker;
    }

    public boolean draining() {
        return draining;
    }

    public int size() {
        return byAgent.size();
    }

    /** A login within this replica's rate; refused ones are told to come back later (4013). */
    public boolean admit() {
        return !draining && admission.tryTake(System.nanoTime());
    }

    /** Opens a session for a verified identity, or null past the tenant's cap. */
    public AgentSession open(Identity id, WebSocketSession socket, jakarta.websocket.Session nativeSocket) {
        AtomicInteger n = perTenant.computeIfAbsent(id.tenantId(), t -> new AtomicInteger());
        if (n.incrementAndGet() > deps.agents().maxSessionsPerTenant()) {
            n.decrementAndGet();
            return null;
        }
        AgentSession s = new AgentSession(id, socket, nativeSocket, this, deps);
        AgentSession previous = byAgent.put(s.key(), s);
        if (previous != null) {
            remove(previous);
            previous.supersede();
        }
        s.start();
        return s;
    }

    void remove(AgentSession s) {
        if (!s.markRemoved()) {
            return;
        }
        byAgent.remove(s.key(), s);
        byInteraction.values().forEach(set -> set.remove(s));
        AtomicInteger n = perTenant.get(s.identity().tenantId());
        if (n != null) {
            n.updateAndGet(v -> Math.max(0, v - 1));
        }
    }

    void index(String interactionId, AgentSession s) {
        byInteraction.computeIfAbsent(interactionId, k -> ConcurrentHashMap.newKeySet()).add(s);
    }

    void unindex(String interactionId, AgentSession s) {
        Set<AgentSession> set = byInteraction.get(interactionId);
        if (set != null) {
            set.remove(s);
            if (set.isEmpty()) {
                byInteraction.remove(interactionId, set);
            }
        }
    }

    void ack(WatchResponse m) {
        acker.accept(m);
    }

    /** True when some session here shows this interaction: the cheap test before an event's body. */
    public boolean watches(String interactionId) {
        Set<AgentSession> set = byInteraction.get(interactionId);
        return set != null && !set.isEmpty();
    }

    /** RE's message for an agent: to its session here, or acknowledged at once. */
    public void onRe(WatchResponse m) {
        AgentSession s = byAgent.get(key(m.getTenantId(), m.getAgentId()));
        if (s == null) {
            acker.accept(m);
        } else {
            s.onRe(m);
        }
    }

    /** An interaction.events event: only to sessions of the event's tenant. */
    public void onEvent(String tenantId, String interactionId, long ownerEpoch, long seq) {
        Set<AgentSession> set = byInteraction.get(interactionId);
        if (set == null) {
            return;
        }
        for (AgentSession s : set) {
            if (s.identity().tenantId().equals(tenantId)) {
                s.onEvent(interactionId, ownerEpoch, seq);
            }
        }
    }

    /** After a stream reopens or a new RE leader: re-attach every live session, at most 500/s. */
    public void reattachAll() {
        List<AgentSession> all = List.copyOf(byAgent.values());
        Thread.ofVirtual().name("reattach").start(() -> {
            for (int i = 0; i < all.size(); i++) {
                all.get(i).reattach();
                if (i % 500 == 499) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        return;
                    }
                }
            }
        });
    }

    /** Pings, token expiry and credential renewal, for every session. */
    public void tickAll() {
        long now = System.currentTimeMillis();
        byAgent.values().forEach(s -> s.tick(now));
    }

    @Override
    public boolean drain(Duration timeout) throws InterruptedException {
        draining = true;
        long deadline = System.nanoTime() + timeout.toNanos();
        long spread = deps.agents().drainSpread().toMillis();
        List<AgentSession> all = List.copyOf(byAgent.values());
        log.atInfo().addKeyValue("event", "sessions.draining").addKeyValue("sessions", all.size()).addKeyValue("spread_ms", spread)
                .log("draining agent sessions");
        all.forEach(s -> s.reconnect(ThreadLocalRandom.current().nextLong(spread + 1)));
        long closeAt = deadline - Duration.ofSeconds(3).toNanos();
        while (!byAgent.isEmpty() && System.nanoTime() < closeAt) {
            Thread.sleep(100);
        }
        List<AgentSession> left = List.copyOf(byAgent.values());
        left.forEach(s -> s.close(1001, "drain"));
        while (!byAgent.isEmpty() && System.nanoTime() < deadline) {
            Thread.sleep(50);
        }
        log.atInfo().addKeyValue("event", "sessions.drained").addKeyValue("closed_at_timeout", left.size())
                .log("agent sessions drained");
        return byAgent.isEmpty();
    }
}
