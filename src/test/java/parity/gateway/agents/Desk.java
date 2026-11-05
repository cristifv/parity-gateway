package parity.gateway.agents;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

/** A scripted desktop: agent-ws.v1 over the JDK's WebSocket client. */
public final class Desk implements WebSocket.Listener {

    public static final HttpClient HTTP = HttpClient.newHttpClient();
    static final JsonMapper JSON = JsonMapper.builder().build();

    public final BlockingQueue<JsonNode> frames = new LinkedBlockingQueue<>();
    public final CompletableFuture<Integer> closed = new CompletableFuture<>();
    public volatile String closeReason = "";
    private final StringBuilder partial = new StringBuilder();
    public WebSocket ws;

    public static Desk open(int port, String hostTenant) throws Exception {
        Desk d = new Desk();
        d.ws = HTTP.newWebSocketBuilder().header("X-Forwarded-Host", hostTenant + ".eu1.platform.example")
                .subprotocols("parity.agent.v1").connectTimeout(Duration.ofSeconds(5))
                .buildAsync(URI.create("ws://127.0.0.1:" + port + "/agents/v1/ws"), d).get(10, TimeUnit.SECONDS);
        return d;
    }

    public void send(String json) {
        ws.sendText(json, true).join();
    }

    public void auth(String token) {
        send("{\"v\":1,\"type\":\"auth\",\"token\":\"" + token + "\"}");
    }

    /** Sends a command with a fresh id and returns the id. */
    public String command(String type, String fields) {
        String id = UUID.randomUUID().toString();
        send("{\"v\":1,\"type\":\"" + type + "\",\"id\":\"" + id + "\"" + (fields.isEmpty() ? "" : "," + fields) + "}");
        return id;
    }

    /** The next frame of {@code type}, skipping others (pings answered). */
    public JsonNode next(String type) throws InterruptedException {
        return next(type, Duration.ofSeconds(5));
    }

    public JsonNode next(String type, Duration within) throws InterruptedException {
        for (var it = backlog.iterator(); it.hasNext(); ) {
            JsonNode f = it.next();
            if (type.equals(f.path("type").asString())) {
                it.remove();
                return f;
            }
        }
        long end = System.nanoTime() + within.toNanos();
        while (System.nanoTime() < end) {
            JsonNode f = frames.poll(Math.max(1, (end - System.nanoTime()) / 1_000_000), TimeUnit.MILLISECONDS);
            if (f == null) {
                break;
            }
            if ("ping".equals(f.path("type").asString())) {
                send("{\"v\":1,\"type\":\"pong\"}");
            }
            if (type.equals(f.path("type").asString())) {
                return f;
            }
            backlog.add(f);
        }
        throw new AssertionError("no " + type + " frame");
    }

    /** Frames read while looking for another type, kept in order for later. */
    private final java.util.List<JsonNode> backlog = new java.util.ArrayList<>();

    /** The result for {@code id}. */
    public JsonNode result(String id) throws InterruptedException {
        java.util.List<JsonNode> others = new java.util.ArrayList<>();
        try {
            while (true) {
                JsonNode r = next("result");
                if (id.equals(r.path("id").asString())) {
                    return r;
                }
                others.add(r);
            }
        } finally {
            backlog.addAll(0, others);
        }
    }

    public void close() {
        ws.sendClose(1000, "bye").join();
    }

    @Override
    public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
        partial.append(data);
        if (last) {
            frames.add(JSON.readTree(partial.toString()));
            partial.setLength(0);
        }
        webSocket.request(1);
        return null;
    }

    @Override
    public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
        closeReason = reason;
        closed.complete(statusCode);
        return null;
    }

    @Override
    public void onError(WebSocket webSocket, Throwable error) {
        closed.complete(-1);
    }

    static ObjectNode obj() {
        return JSON.createObjectNode();
    }
}
