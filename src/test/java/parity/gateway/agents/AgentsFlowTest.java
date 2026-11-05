package parity.gateway.agents;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;
import parity.contracts.agents.v1.AgentState;
import parity.gateway.agents.ie.EventsConsumer;
import parity.gateway.fake.FakeInteractionEngine;
import parity.gateway.fake.FakeRoutingEngine;
import tools.jackson.databind.JsonNode;

/**
 * Steps 2–4's main behaviour through the real path: a scripted desktop over the WebSocket, the
 * gateway, the fake RE (AgentService) and the fake IE (InteractionControlService). One tenant per
 * test so their queues don't meet.
 */
class AgentsFlowTest {

    static FakeRoutingEngine re;
    static FakeInteractionEngine ie;
    static TestKeys keys;
    static ConfigurableApplicationContext gw;
    static int port;

    @BeforeAll
    static void start() throws Exception {
        keys = new TestKeys();
        re = new FakeRoutingEngine(System::currentTimeMillis, 15_000, 60_000).start(0, true);
        ie = new FakeInteractionEngine().start(0);
        gw = Replicas.start("gw-flow-1", re, ie, keys, Map.of());
        port = Replicas.port(gw);
    }

    @AfterAll
    static void stop() {
        gw.close();
        re.close();
        ie.close();
    }

    static Desk login(String tenant, String agent) throws Exception {
        Desk d = Desk.open(port, tenant);
        d.auth(keys.token(tenant, agent));
        JsonNode snap = d.next("snapshot");
        assertEquals("not_ready", snap.get("agent").get("state").asString());
        return d;
    }

    static void ok(Desk d, String id) throws Exception {
        JsonNode r = d.result(id);
        assertTrue(r.get("ok").asBoolean(), r.toString());
    }

    static String error(Desk d, String id) throws Exception {
        JsonNode r = d.result(id);
        assertFalse(r.get("ok").asBoolean(), r.toString());
        return r.get("error").get("code").asString();
    }

    @Test
    void offerAcceptControlsEventsAndWrapUp() throws Exception {
        Desk d = login("acme", "agent-a1");
        assertTrue(d.next("sip_credentials").get("token").asString().length() > 20);
        ok(d, d.command("set_state", "\"state\":\"ready\""));
        String interaction = Uuid7.next();
        re.queueCall("acme", interaction, "sales");
        JsonNode offer = d.next("offer");
        assertEquals(interaction, offer.get("interaction_id").asString());
        ok(d, d.command("accept", "\"offer_id\":\"" + offer.get("offer_id").asString() + "\""));
        JsonNode reserved = d.next("reserved");
        assertEquals(interaction, reserved.get("interaction_id").asString(), "the softphone may auto-answer this one");
        String reservation = reserved.get("reservation_id").asString();
        ie.seedCall("acme", interaction, reservation, "agent-a1", UUID.randomUUID().toString(),
                UUID.randomUUID().toString(), true, 1, 1);
        String call = "\"interaction_id\":\"" + interaction + "\"";

        ok(d, d.command("hold", call));
        assertTrue(d.next("call").get("call").get("held").asBoolean());
        ok(d, d.command("mute", call));
        assertTrue(d.next("call").get("call").get("muted").asBoolean(), "mute goes through IE (parity-ms)");
        assertEquals("not_your_call", error(d, d.command("hangup", "\"interaction_id\":\"" + UUID.randomUUID() + "\"")),
                "an interaction the session doesn't know is refused before IE");
        String same = d.command("resume", call);
        ok(d, same);
        d.send("{\"v\":1,\"type\":\"resume\",\"id\":\"" + same + "\"," + call + "}");
        ok(d, same);
        assertFalse(d.next("call").get("call").get("held").asBoolean());
        assertFalse(d.next("call").get("call").get("held").asBoolean(), "a repeat gets IE's first result");

        // interaction.events: routed to this session only when header and body agree.
        EventsConsumer events = gw.getBean(EventsConsumer.class);
        assertEquals("header_body_mismatch", events.handle(record("globex", interaction, "acme", 9)));
        assertEquals("not_watched", events.handle(record("acme", UUID.randomUUID().toString(), "acme", 9)));

        ok(d, d.command("transfer", call + ",\"target\":{\"queue_id\":\"billing\"}"));
        d.next("call");
        assertEquals("not_your_call", error(d, d.command("hangup", call)),
                "after a transfer the agent holds neither the reservation nor a live leg");
        assertEquals("routed", events.handle(record("acme", interaction, "acme", 99)));
        assertEquals("ended", d.next("call").get("call").get("state").asString(), "IE's view after the event");

        re.callEnded("acme", reservation);
        assertEquals(reservation, d.next("wrap_up").get("reservation_id").asString());
        ok(d, d.command("wrap_up_done", "\"reservation_id\":\"" + reservation + "\",\"disposition_code\":\"sale\""));
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (re.stateOf("acme", "agent-a1") != AgentState.AGENT_STATE_READY && System.nanoTime() < end) {
            Thread.sleep(20);
        }
        assertEquals(AgentState.AGENT_STATE_READY, re.stateOf("acme", "agent-a1"));
        d.close();
    }

    static ConsumerRecord<byte[], byte[]> record(String headerTenant, String interaction, String bodyTenant, long seq) {
        String body = "{\"tenant_id\":\"" + bodyTenant + "\",\"interaction_id\":\"" + interaction
                + "\",\"owner_epoch\":5,\"seq\":" + seq + ",\"type\":\"hold_started\"}";
        ConsumerRecord<byte[], byte[]> r = new ConsumerRecord<>("interaction.events", 0, 0,
                interaction.getBytes(StandardCharsets.UTF_8), body.getBytes(StandardCharsets.UTF_8));
        r.headers().add("tenant_id", headerTenant.getBytes(StandardCharsets.UTF_8));
        r.headers().add("interaction_id", interaction.getBytes(StandardCharsets.UTF_8));
        return r;
    }

    @Test
    void theTenantComesFromTheTokenAndMustMatchTheHost() throws Exception {
        Desk wrongHost = Desk.open(port, "globex");
        wrongHost.auth(keys.token("acme", "agent-a2"));
        assertEquals(4003, wrongHost.closed.get(5, TimeUnit.SECONDS));
        Desk silent = Desk.open(port, "acme");
        assertEquals(4008, silent.closed.get(5, TimeUnit.SECONDS));
        Desk noRole = Desk.open(port, "acme");
        noRole.auth(keys.token("acme", "agent-a2", java.util.List.of("supervisor"), 60_000));
        assertEquals(4001, noRole.closed.get(5, TimeUnit.SECONDS));
        Desk injected = login("acme", "agent-a2");
        String id = injected.command("set_state", "\"state\":\"ready\",\"tenant_id\":\"globex\"");
        assertEquals("bad_request", error(injected, id), "no frame may name a tenant");
        injected.close();
    }

    @Test
    void aSecondTabSupersedesTheFirst() throws Exception {
        Desk first = login("acme", "agent-a3");
        Desk second = login("acme", "agent-a3");
        assertEquals(4009, first.closed.get(5, TimeUnit.SECONDS));
        ok(second, second.command("set_state", "\"state\":\"ready\""));
        second.close();
    }

    /**
     * CR-053, the gateway's part: an Offer lost on the way (RE holds it) reaches the desktop from
     * the re-attach snapshot when the stream comes back, and the agent can accept it.
     */
    @Test
    void aReattachTakesALostOfferFromTheSnapshot() throws Exception {
        Desk d = login("initech-uk", "agent-u1");
        ok(d, d.command("set_state", "\"state\":\"ready\""));
        re.loseNextOffer("initech-uk", "agent-u1");
        String interaction = Uuid7.next();
        re.queueCall("initech-uk", interaction, "sales");
        Thread.sleep(700);
        assertTrue(d.frames.stream().noneMatch(f -> "offer".equals(f.get("type").asString())),
                "the Offer message was lost");
        re.dropStreams();
        JsonNode offer = d.next("offer", Duration.ofSeconds(10));
        assertEquals(interaction, offer.get("interaction_id").asString(), "taken from the re-attach snapshot");
        ok(d, d.command("accept", "\"offer_id\":\"" + offer.get("offer_id").asString() + "\""));
        assertEquals(interaction, d.next("reserved").get("interaction_id").asString());
        d.close();
    }

    @Test
    void aDroppedStreamReattachesAndADetachedAgentGetsNoOffers() throws Exception {
        Desk d = login("initech-de", "agent-i1");
        ok(d, d.command("set_state", "\"state\":\"ready\""));
        re.dropStreams();
        Thread.sleep(1500);
        re.queueCall("initech-de", Uuid7.next(), "sales");
        JsonNode offer = d.next("offer", Duration.ofSeconds(10));
        ok(d, d.command("reject", "\"offer_id\":\"" + offer.get("offer_id").asString() + "\""));
        d.close();
        d.closed.get(5, TimeUnit.SECONDS);
        Thread.sleep(500);
        int before = re.queued();
        re.queueCall("initech-de", Uuid7.next(), "sales");
        Thread.sleep(500);
        assertEquals(before + 1, re.queued(), "a detached agent gets no offer; the calls wait");
    }
}
