package parity.gateway.agents;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServiceDescriptor;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import parity.contracts.agents.v1.AgentServiceGrpc;
import parity.contracts.agents.v1.AgentState;
import parity.contracts.agents.v1.Reservation;
import parity.contracts.interaction.v1.InteractionControlServiceGrpc;
import parity.gateway.agents.ws.Frames;
import parity.gateway.fake.FakeInteractionEngine;
import parity.gateway.fake.FakeRoutingEngine;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

/**
 * parity-contracts 0.3.0's gateway fixtures: the command_id derivation, the agent-ws.v1 frames
 * through the gateway's parser, and every agents.v1 and interaction.v1 script played against the
 * fake RE and fake IE. Scripts compare semantically: ids and times RE makes are mapped or ignored.
 */
class ContractFixturesTest {

    static final Path CONTRACTS = Path.of(System.getProperty("parity.contracts.dir", "../parity-contracts"));
    static final JsonMapper JSON = JsonMapper.builder().build();
    static final Set<String> VOLATILE = Set.of("since_ms", "state_version", "offer_id", "reservation_id",
            "expires_at_ms", "deadline_ms", "ends_at_ms", "leader_epoch", "delivery_seq", "at_ms", "leg_id", "seq",
            "owner_epoch");

    static List<Path> fixtures(String dir) throws Exception {
        try (Stream<Path> s = Files.walk(CONTRACTS.resolve("fixtures").resolve(dir))) {
            return s.filter(p -> p.toString().endsWith(".json")).sorted().toList();
        }
    }

    @Test
    void commandIdsMatchTheFixtures() throws Exception {
        JsonNode script = JSON.readTree(CONTRACTS.resolve("fixtures/agents.v1/valid/attach-offer-accept.json").toFile());
        String expected = script.get("steps").get(2).get("body").get("command_id").asString();
        assertEquals(expected, CommandIds.derive("acme", "agent-7f3a", "0192f6a1-c000-7aaa-8bbb-000000000001"));
        assertEquals(46, expected.length());
        assertTrue(!CommandIds.derive("acme", "agent-7f3", "a0192f6a1-c000-7aaa-8bbb-000000000001".substring(1))
                .equals(CommandIds.derive("acme", "agent-7f", "3a0192f6a1-c000-7aaa-8bbb-000000000001".substring(2))));
        assertThrows(IllegalArgumentException.class, () -> CommandIds.derive("acme", "a", "not-a-uuid"));
    }

    @TestFactory
    Stream<DynamicTest> desktopFramesParseAsTheSchemaSays() throws Exception {
        List<DynamicTest> out = new ArrayList<>();
        for (Path p : fixtures("agent-ws.v1")) {
            JsonNode f = JSON.readTree(p.toFile());
            if (!"to_gateway".equals(f.get("direction").asString())) {
                continue;
            }
            out.add(DynamicTest.dynamicTest(p.getFileName().toString(), () -> {
                String text = f.get("frame").toString();
                if (f.get("writer_valid").asBoolean()) {
                    assertNotNull(Frames.parse(text).type());
                } else {
                    assertThrows(Frames.Invalid.class, () -> Frames.parse(text));
                }
            }));
        }
        assertTrue(out.size() >= 9);
        return out.stream();
    }

    @TestFactory
    Stream<DynamicTest> agentsScripts() throws Exception {
        // contracts 0.11.0: scripts needing routing-engine stimuli (config, pins, moves, a connected
        // call) are parity-cc-routing's; the fake RE isn't a conformance target (its PLAN §7).
        return fixtures("agents.v1").stream().filter(ContractFixturesTest::fakeCanPlay)
                .map(p -> DynamicTest.dynamicTest(p.getFileName().toString(), () -> play(p)));
    }

    private static boolean fakeCanPlay(Path p) {
        try {
            JsonNode script = JSON.readTree(p.toFile());
            if (script.path("given").has("routing") || script.path("given").has("routing_pins")) {
                return false;  // a tenant's routing policy and pins: RE's
            }
            for (JsonNode step : script.get("steps")) {
                if ("inject".equals(step.path("op").asString())
                        && !Set.of("queue_call", "call_ended").contains(step.path("what").asString())) {
                    return false;
                }
            }
            return true;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @TestFactory
    Stream<DynamicTest> interactionScripts() throws Exception {
        return fixtures("interaction.v1").stream().map(p -> DynamicTest.dynamicTest(p.getFileName().toString(), () -> play(p)));
    }

    // ---- the runner ---------------------------------------------------------------------

    private void play(Path file) throws Exception {
        JsonNode script = JSON.readTree(file.toFile());
        JsonNode given = script.get("given");
        boolean agents = "parity.agents.v1".equals(script.get("package").asString());
        AtomicLong clock = new AtomicLong(given.path("start_ms").asLong(1792800000000L));
        FakeRoutingEngine re = null;
        FakeInteractionEngine ie = null;
        ServiceDescriptor service;
        int port;
        if (agents) {
            re = new FakeRoutingEngine(clock::get, given.get("offer_timeout_ms").asLong(), given.get("grace_ms").asLong())
                    .start(0, false);
            for (JsonNode a : given.get("agents")) {
                Reservation res = null;
                if (a.has("reservation")) {
                    res = Reservation.newBuilder().setReservationId(a.get("reservation").get("reservation_id").asString())
                            .setInteractionId(a.get("reservation").get("interaction_id").asString()).build();
                }
                re.seedAgent(a.get("tenant_id").asString(), a.get("agent_id").asString(),
                        AgentState.valueOf("AGENT_STATE_" + a.get("state").asString()), a.path("reason_code").asString(""), res);
            }
            port = re.port();
            service = AgentServiceGrpc.getServiceDescriptor();
        } else {
            FakeInteractionEngine created = new FakeInteractionEngine().start(0);
            ie = created;
            for (JsonNode c : given.get("calls")) {
                created.seedCall(c.get("tenant_id").asString(), c.get("interaction_id").asString(), c.get("reservation_id").asString(),
                        c.get("agent_id").asString(), c.get("customer_leg_id").asString(), c.get("agent_leg_id").asString(),
                        c.get("managed").asBoolean(), c.get("owner_epoch").asLong(), c.get("seq").asLong());
            }
            given.get("blocked_numbers").forEach(n -> created.block(n.asString()));
            created.ownerMoving(given.path("owner_moving").asBoolean(false));
            port = ie.port();
            service = InteractionControlServiceGrpc.getServiceDescriptor();
        }
        ManagedChannel channel = NettyChannelBuilder.forAddress("127.0.0.1", port).usePlaintext().build();
        BlockingQueue<Message> stream = new LinkedBlockingQueue<>();
        Map<String, String> ids = new HashMap<>();
        try {
            int i = 0;
            for (JsonNode step : script.get("steps")) {
                String where = file.getFileName() + " step " + i++;
                switch (step.get("op").asString()) {
                    case "call" -> call(channel, service, step, stream, ids, where);
                    case "deliver" -> deliver(step, stream, ids, where);
                    case "advance" -> {
                        clock.addAndGet(step.get("ms").asLong());
                        re.tick();
                    }
                    case "inject" -> {
                        JsonNode a = step.get("args");
                        if ("queue_call".equals(step.get("what").asString())) {
                            re.queueCall(a.get("tenant_id").asString(), a.get("interaction_id").asString(), a.get("queue_id").asString());
                        } else {
                            re.callEnded(a.get("tenant_id").asString(), map(ids, a.get("reservation_id").asString()));
                        }
                    }
                    default -> fail(where + ": unknown op");
                }
            }
        } finally {
            channel.shutdownNow();
            if (re != null) {
                re.close();
            }
            if (ie != null) {
                ie.close();
            }
        }
    }

    private static String map(Map<String, String> ids, String id) {
        return ids.getOrDefault(id, id);
    }

    @SuppressWarnings("unchecked")
    private static void call(ManagedChannel channel, ServiceDescriptor service, JsonNode step, BlockingQueue<Message> stream,
            Map<String, String> ids, String where) throws Exception {
        String name = step.get("method").asString();
        MethodDescriptor<Message, Message> md = (MethodDescriptor<Message, Message>) service.getMethods().stream()
                .filter(m -> m.getFullMethodName().equals(name)).findFirst().orElseThrow();
        Message proto = ((MethodDescriptor.PrototypeMarshaller<Message>) md.getRequestMarshaller()).getMessagePrototype();
        String body = step.get("body").toString();
        for (var e : ids.entrySet()) {
            body = body.replace(e.getKey(), e.getValue());
        }
        Message.Builder b = proto.newBuilderForType();
        JsonFormat.parser().merge(body, b);
        Metadata md2 = new Metadata();
        md2.put(GrpcCalls.TEST_PEER, "parity-gateway.bin-a");
        step.path("metadata").properties().forEach(e ->
                md2.put(Metadata.Key.of(e.getKey(), Metadata.ASCII_STRING_MARSHALLER), e.getValue().asString()));
        Channel ch = ClientInterceptors.intercept(channel, MetadataUtils.newAttachHeadersInterceptor(md2));
        JsonNode expect = step.get("expect");
        if (md.getType() == MethodDescriptor.MethodType.SERVER_STREAMING) {
            ClientCalls.asyncServerStreamingCall(ch.newCall(md, CallOptions.DEFAULT), b.build(), new StreamObserver<>() {
                @Override
                public void onNext(Message m) {
                    stream.add(m);
                }

                @Override
                public void onError(Throwable t) {}

                @Override
                public void onCompleted() {}
            });
            assertNotNull(stream.poll(2, TimeUnit.SECONDS), where + ": the stream opens with a heartbeat");
            return;
        }
        try {
            Message got = ClientCalls.blockingUnaryCall(ch, md, CallOptions.DEFAULT.withDeadlineAfter(2, TimeUnit.SECONDS), b.build());
            assertTrue(expect.has("message"), where + ": expected " + expect + ", got an answer");
            compare(expect.get("body"), got, ids, where);
        } catch (StatusRuntimeException e) {
            assertTrue(expect.has("status"), where + ": unexpected " + e.getStatus());
            assertEquals(expect.get("status").asString(), e.getStatus().getCode().name(), where);
            assertEquals(expect.get("parity_error").asString(), e.getTrailers().get(GrpcCalls.PARITY_ERROR), where);
        }
    }

    private static void deliver(JsonNode step, BlockingQueue<Message> stream, Map<String, String> ids, String where)
            throws Exception {
        JsonNode body = step.get("body");
        String event = null;
        for (String k : List.of("offer", "offer_revoked", "reserved", "state_changed", "wrap_up_started", "detached")) {
            if (body.has(k)) {
                event = k;
            }
        }
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (System.nanoTime() < end) {
            Message m = stream.poll(100, TimeUnit.MILLISECONDS);
            if (m != null && json(m).has(event)) {
                compare(body, m, ids, where);
                return;
            }
        }
        fail(where + ": no " + event + " delivered");
    }

    private static JsonNode json(Message m) throws Exception {
        return JSON.readTree(JsonFormat.printer().preservingProtoFieldNames().print(m));
    }

    private static void compare(JsonNode expectedBody, Message got, Map<String, String> ids, String where) throws Exception {
        Message.Builder b = got.newBuilderForType();
        JsonFormat.parser().merge(expectedBody.toString(), b);
        JsonNode want = json(b.build());
        JsonNode have = json(got);
        learn(want, have, ids);
        assertEquals(strip(want), strip(have), where);
    }

    private static void learn(JsonNode want, JsonNode have, Map<String, String> ids) {
        if (want == null || have == null || !want.isObject()) {
            return;
        }
        for (String k : want.propertyNames()) {
            JsonNode w = want.get(k);
            JsonNode h = have.get(k);
            if (("offer_id".equals(k) || "reservation_id".equals(k)) && h != null && !w.asString().isEmpty()) {
                ids.putIfAbsent(w.asString(), h.asString());
            }
            learn(w, h, ids);
        }
    }

    private static JsonNode strip(JsonNode n) {
        if (n.isObject()) {
            ObjectNode o = JSON.createObjectNode();
            for (String k : n.propertyNames()) {
                if (!VOLATILE.contains(k)) {
                    o.set(k, strip(n.get(k)));
                }
            }
            return o;
        }
        if (n.isArray()) {
            var a = JSON.createArrayNode();
            n.forEach(x -> a.add(strip(x)));
            return a;
        }
        return n;
    }
}
