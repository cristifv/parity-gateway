package parity.gateway.e2e;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

/** etcd v3's JSON gateway: what IE writes for TC (replica registrations and owner records). */
final class Etcd {

    private static final JsonMapper JSON = JsonMapper.builder().build();

    private final URI base;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();

    Etcd(URI base) {
        this.base = base;
    }

    void put(String key, String value) throws IOException, InterruptedException {
        ObjectNode b = JSON.createObjectNode().put("key", b64(key)).put("value", b64(value));
        var unused = post("/v3/kv/put", b);
    }

    /**
     * Writes {@code value} only if {@code key} is absent and returns its create revision; when the
     * key exists, returns its create revision only if the stored value is exactly {@code value}
     * (all of tenant, bin, replica and node match), else -1 (someone else's record).
     */
    long createIfAbsent(String key, String value) throws IOException, InterruptedException {
        ObjectNode t = JSON.createObjectNode();
        t.putArray("compare").addObject().put("key", b64(key)).put("target", "CREATE").put("result", "EQUAL")
                .put("create_revision", "0");
        t.putArray("success").addObject().putObject("request_put").put("key", b64(key)).put("value", b64(value));
        t.putArray("failure").addObject().putObject("request_range").put("key", b64(key));
        JsonNode r = post("/v3/kv/txn", t);
        if (r.path("succeeded").asBoolean(false)) {
            return Long.parseLong(r.path("header").path("revision").asString());
        }
        JsonNode kv = r.path("responses").path(0).path("response_range").path("kvs").path(0);
        String stored = new String(Base64.getDecoder().decode(kv.path("value").asString("")), StandardCharsets.UTF_8);
        return stored.equals(value) ? Long.parseLong(kv.path("create_revision").asString("-1")) : -1;
    }

    /** Deletes {@code key} only while it still holds exactly {@code value}; true when deleted. */
    boolean deleteIfValue(String key, String value) throws IOException, InterruptedException {
        ObjectNode t = JSON.createObjectNode();
        t.putArray("compare").addObject().put("key", b64(key)).put("target", "VALUE").put("result", "EQUAL")
                .put("value", b64(value));
        t.putArray("success").addObject().putObject("request_delete_range").put("key", b64(key));
        return post("/v3/kv/txn", t).path("succeeded").asBoolean(false);
    }

    boolean healthy() {
        try {
            return http.send(HttpRequest.newBuilder(base.resolve("/health")).timeout(Duration.ofSeconds(1)).GET().build(),
                    HttpResponse.BodyHandlers.ofString()).statusCode() == 200;
        } catch (IOException e) {
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private JsonNode post(String path, ObjectNode body) throws IOException, InterruptedException {
        HttpResponse<String> r = http.send(HttpRequest.newBuilder(base.resolve(path)).timeout(Duration.ofSeconds(5))
                .POST(HttpRequest.BodyPublishers.ofString(body.toString())).build(), HttpResponse.BodyHandlers.ofString());
        if (r.statusCode() != 200) {
            throw new IOException("etcd " + path + " answered " + r.statusCode());
        }
        return JSON.readTree(r.body());
    }

    private static String b64(String s) {
        return Base64.getEncoder().encodeToString(s.getBytes(StandardCharsets.UTF_8));
    }
}
