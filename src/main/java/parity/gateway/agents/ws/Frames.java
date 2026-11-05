package parity.gateway.agents.ws;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import parity.gateway.agents.CommandIds;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

/**
 * {@code agent-ws.v1} desktop → gateway frames (parity-contracts
 * {@code schemas/agent-ws/to-gateway.v1.json}), checked by hand with the schema's rules: no
 * unknown field, a UUID {@code id} on every command, no tenant or agent anywhere.
 */
public final class Frames {

    /** A parsed frame; {@code type} null for one the gateway ignores (an unknown type). */
    public record Frame(String type, String id, JsonNode body) {
        public String text(String field) {
            JsonNode n = body.get(field);
            return n == null ? null : n.asString();
        }
    }

    /** A frame that breaks the schema; {@code id} when it carried a valid one. */
    public static final class Invalid extends Exception {
        private static final long serialVersionUID = 1L;
        private final java.util.Optional<String> id;

        Invalid(java.util.Optional<String> id) {
            super("invalid frame", null, false, false);
            this.id = id;
        }

        public String id() {
            return id.orElse(null);
        }
    }

    public static final JsonMapper JSON = JsonMapper.builder().build();
    public static final int MAX_BYTES = 16 * 1024;

    private static final Pattern UUID = Pattern.compile("^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$");
    private static final Pattern CODE = Pattern.compile("^[A-Za-z0-9_.-]{1,64}$");
    private static final Pattern OPAQUE = Pattern.compile("^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$");
    private static final Pattern PHONE = Pattern.compile("^\\+[1-9][0-9]{6,14}$");
    private static final Set<String> CALL_CONTROLS = Set.of("hold", "resume", "mute", "unmute", "hangup", "merge",
            "complete_transfer", "cancel_consult");

    /** Per type: the required fields, then the optional ones ({@code v} and {@code type} implied). */
    private static final Map<String, String[][]> FIELDS = new HashMap<>();

    static {
        FIELDS.put("auth", new String[][] {{"token"}, {"client_version"}});
        FIELDS.put("set_state", new String[][] {{"id", "state"}, {"reason_code"}});
        FIELDS.put("accept", new String[][] {{"id", "offer_id"}, {}});
        FIELDS.put("reject", new String[][] {{"id", "offer_id"}, {"reason"}});
        FIELDS.put("transfer", new String[][] {{"id", "interaction_id", "target"}, {}});
        FIELDS.put("consult", new String[][] {{"id", "interaction_id", "target"}, {}});
        FIELDS.put("remove_party", new String[][] {{"id", "interaction_id", "leg_id"}, {}});
        FIELDS.put("wrap_up_done", new String[][] {{"id", "reservation_id", "disposition_code"}, {}});
        FIELDS.put("pong", new String[][] {{}, {}});
        for (String c : CALL_CONTROLS) {
            FIELDS.put(c, new String[][] {{"id", "interaction_id"}, {}});
        }
    }

    private Frames() {}

    public static boolean isCallControl(String type) {
        return CALL_CONTROLS.contains(type) || "transfer".equals(type) || "consult".equals(type)
                || "remove_party".equals(type);
    }

    public static Frame parse(String text) throws Invalid {
        if (text.length() > MAX_BYTES) {
            throw new Invalid(java.util.Optional.empty());
        }
        JsonNode root;
        try {
            root = JSON.readTree(text);
        } catch (JacksonException e) {
            throw new Invalid(java.util.Optional.empty());
        }
        if (root == null || !root.isObject() || root.path("v").asInt(0) != 1 || !root.path("type").isString()) {
            throw new Invalid(java.util.Optional.empty());
        }
        String type = root.get("type").asString();
        JsonNode idNode = root.get("id");
        String id = idNode != null && idNode.isString() && CommandIds.isUuid(idNode.asString()) ? idNode.asString() : null;
        String[][] spec = FIELDS.get(type);
        if (spec == null) {
            return new Frame(null, id, root);
        }
        Set<String> allowed = new java.util.HashSet<>(Set.of("v", "type"));
        allowed.addAll(Set.of(spec[0]));
        allowed.addAll(Set.of(spec[1]));
        for (var name : root.propertyNames()) {
            if (!allowed.contains(name) || !valid(name, root.get(name))) {
                throw new Invalid(java.util.Optional.ofNullable(id));
            }
        }
        for (String required : spec[0]) {
            if (!root.has(required)) {
                throw new Invalid(java.util.Optional.ofNullable(id));
            }
        }
        return new Frame(type, id, root);
    }

    private static boolean valid(String name, JsonNode v) {
        return switch (name) {
            case "v", "type" -> true;
            case "id", "offer_id", "interaction_id", "reservation_id", "leg_id" -> matches(v, UUID);
            case "state" -> v.isString() && Set.of("ready", "not_ready", "logged_out").contains(v.asString());
            case "reason_code", "reason", "disposition_code" -> matches(v, CODE);
            case "token" -> v.isString() && v.asString().length() >= 16 && v.asString().length() <= 8192;
            case "client_version" -> v.isString() && v.asString().length() <= 64;
            case "target" -> validTarget(v);
            default -> false;
        };
    }

    private static boolean validTarget(JsonNode t) {
        if (!t.isObject() || t.size() != 1) {
            return false;
        }
        String key = t.propertyNames().iterator().next();
        JsonNode v = t.get(key);
        return switch (key) {
            case "queue_id" -> matches(v, CODE);
            case "agent_id" -> matches(v, OPAQUE);
            case "number" -> matches(v, PHONE);
            default -> false;
        };
    }

    private static boolean matches(JsonNode v, Pattern p) {
        return v != null && v.isString() && p.matcher(v.asString()).matches();
    }

    /** A gateway → desktop frame of {@code type}; the outbox adds {@code seq}. */
    public static ObjectNode out(String type) {
        ObjectNode n = JSON.createObjectNode();
        n.put("v", 1);
        n.put("type", type);
        return n;
    }
}
