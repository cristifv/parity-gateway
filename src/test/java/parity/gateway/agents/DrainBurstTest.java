package parity.gateway.agents;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;
import parity.gateway.agents.session.SessionRegistry;
import parity.gateway.fake.FakeInteractionEngine;
import parity.gateway.fake.FakeRoutingEngine;
import tools.jackson.databind.JsonNode;

/**
 * Step 4's done-when: a drain moves 200 agents to another replica within the 0–10 s spread with
 * no lost offer, and a burst of 1,000 logins is admitted (rate-limited with 4013 and
 * retry_after_ms, then all in) without errors.
 */
class DrainBurstTest {

    static FakeRoutingEngine re;
    static FakeInteractionEngine ie;
    static TestKeys keys;
    static ConfigurableApplicationContext a;
    static ConfigurableApplicationContext b;

    @BeforeAll
    static void start() throws Exception {
        keys = new TestKeys();
        re = new FakeRoutingEngine(System::currentTimeMillis, 15_000, 60_000).start(0, true);
        ie = new FakeInteractionEngine().start(0);
        a = Replicas.start("gw-a", re, ie, keys, Map.of());
        b = Replicas.start("gw-b", re, ie, keys, Map.of());
    }

    @AfterAll
    static void stop() {
        b.close();
        a.close();
        re.close();
        ie.close();
    }

    /** A desktop that accepts every offer and follows a reconnect to replica B. */
    static final class Agent implements Runnable {
        final String id;
        final AtomicBoolean stop;
        final AtomicInteger accepted;
        final CountDownLatch moved;
        final Set<String> acceptIds = ConcurrentHashMap.newKeySet();
        /** GW-036: accepts whose result frame was still due when the desk was replaced. */
        final AtomicInteger unansweredAtReconnect;
        volatile Desk desk;
        volatile long reconnectAfterMs = -1;

        Agent(String id, AtomicBoolean stop, AtomicInteger accepted, CountDownLatch moved,
                AtomicInteger unansweredAtReconnect) {
            this.id = id;
            this.stop = stop;
            this.accepted = accepted;
            this.moved = moved;
            this.unansweredAtReconnect = unansweredAtReconnect;
        }

        void login(int port) throws Exception {
            desk = Desk.open(port, "umbrella");
            desk.auth(keys.token("umbrella", id));
            desk.send("{\"v\":1,\"type\":\"set_state\",\"id\":\"" + UUID.randomUUID() + "\",\"state\":\"ready\"}");
        }

        private void accept(JsonNode offer) {
            acceptIds.add(desk.command("accept", "\"offer_id\":\"" + offer.get("offer_id").asString() + "\""));
        }

        @Override
        public void run() {
            try {
                while (!stop.get()) {
                    JsonNode f = desk.frames.poll(100, TimeUnit.MILLISECONDS);
                    if (f == null) {
                        continue;
                    }
                    switch (f.path("type").asString()) {
                        case "offer" -> accept(f);
                        case "snapshot" -> {
                            if (f.has("offer")) {
                                accept(f.get("offer"));
                            }
                        }
                        case "result" -> {
                            if (acceptIds.remove(f.get("id").asString()) && f.get("ok").asBoolean()) {
                                accepted.incrementAndGet();
                            }
                        }
                        case "ping" -> desk.send("{\"v\":1,\"type\":\"pong\"}");
                        case "reconnect" -> {
                            reconnectAfterMs = f.get("after_ms").asLong();
                            Thread.sleep(reconnectAfterMs);
                            // GW-036: a result still due goes with the old desk (its frames are
                            // never read again): the accept counts at RE, not here.
                            unansweredAtReconnect.addAndGet(acceptIds.size());
                            acceptIds.clear();
                            desk.close();
                            login(Replicas.port(b));
                            moved.countDown();
                        }
                        default -> {
                            // state, reserved, credentials: nothing to do
                        }
                    }
                }
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }
    }

    @Test
    void aDrainMoves200AgentsWithNoLostOffer() throws Exception {
        AtomicBoolean stop = new AtomicBoolean();
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger unanswered = new AtomicInteger();
        CountDownLatch moved = new CountDownLatch(200);
        List<Agent> agents = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            Agent ag = new Agent("u" + i, stop, accepted, moved, unanswered);
            ag.login(Replicas.port(a));
            agents.add(ag);
        }
        agents.forEach(ag -> Thread.startVirtualThread(ag));
        Thread.sleep(1000);
        SessionRegistry onA = a.getBean(SessionRegistry.class);
        assertEquals(200, onA.size());
        long t0 = System.nanoTime();
        Thread drain = Thread.startVirtualThread(() -> {
            try {
                onA.drain(Duration.ofSeconds(20));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        for (int i = 0; i < 50; i++) {
            re.queueCall("umbrella", Uuid7.next(), "sales");
            Thread.sleep(100);
        }
        assertTrue(moved.await(15, TimeUnit.SECONDS), "every agent reconnected to replica B");
        long movedMs = (System.nanoTime() - t0) / 1_000_000;
        assertTrue(agents.stream().allMatch(ag -> ag.reconnectAfterMs >= 0 && ag.reconnectAfterMs <= 10_000));
        drain.join(Duration.ofSeconds(20));
        assertEquals(0, onA.size(), "replica A has no session left");
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while ((re.reservations("umbrella") < 50 || re.queued() > 0) && System.nanoTime() < end) {
            Thread.sleep(100);
        }
        stop.set(true);
        // GW-036: "no lost offer" is what RE holds: one reservation per queued call, nothing left
        // queued or offered. The desks' result frames are a diagnostic only: an accept sent just
        // before a `reconnect` has its result go to the replaced desk (the reservation is made).
        String seen = "moved in " + movedMs + " ms; RE reservations " + re.reservations("umbrella")
                + ", queued+offered " + re.queued() + "; result frames " + accepted.get()
                + ", results due at a reconnect " + unanswered.get() + ", offers expired " + re.offersExpired()
                + ", agents NOT_READY missed_offer " + re.missedOffer("umbrella");
        System.out.println("GW-036: " + seen);
        assertEquals(50, re.reservations("umbrella"), "every queued call was offered and accepted (" + seen + ")");
        assertEquals(0, re.queued(), "nothing left queued or offered (" + seen + ")");
        // A lost offer the fake heals by its 15 s expiry (inside the 20 s wait) still shows here.
        assertEquals(0, re.missedOffer("umbrella"), "no agent ended NOT_READY missed_offer (" + seen + ")");
        assertEquals(0, re.offersExpired(), "no offer expired unanswered (" + seen + ")");
        assertTrue(accepted.get() + unanswered.get() >= 50,
                "every accept was answered on its desk, or its desk was replaced first (" + seen + ")");
        agents.forEach(ag -> ag.desk.close());
    }

    @Test
    void aBurstOf1000LoginsIsAdmitted() throws Exception {
        int port = Replicas.port(b);
        AtomicInteger busy = new AtomicInteger();
        AtomicInteger failed = new AtomicInteger();
        CountDownLatch in = new CountDownLatch(1000);
        List<Desk> desks = new java.util.concurrent.CopyOnWriteArrayList<>();
        for (int i = 0; i < 1000; i++) {
            String agent = "h" + i;
            Thread.startVirtualThread(() -> {
                try {
                    for (int attempt = 0; attempt < 20; attempt++) {
                        Desk d = Desk.open(port, "hooli");
                        d.auth(keys.token("hooli", agent));
                        while (true) {
                            JsonNode f = d.frames.poll(100, TimeUnit.MILLISECONDS);
                            if (f != null && "snapshot".equals(f.path("type").asString())) {
                                desks.add(d);
                                in.countDown();
                                return;
                            }
                            if (d.closed.isDone()) {
                                break;
                            }
                        }
                        if (d.closed.get() != 4013) {
                            failed.incrementAndGet();
                            return;
                        }
                        busy.incrementAndGet();
                        Thread.sleep(Long.parseLong(d.closeReason.substring("retry_after_ms=".length())));
                    }
                    failed.incrementAndGet();
                } catch (Exception e) {
                    failed.incrementAndGet();
                }
            });
        }
        assertTrue(in.await(40, TimeUnit.SECONDS), "all 1,000 logged in; failed " + failed.get());
        assertEquals(0, failed.get());
        assertTrue(busy.get() > 0, "the admission limit (200/s) was exercised");
        desks.forEach(Desk::close);
    }
}
