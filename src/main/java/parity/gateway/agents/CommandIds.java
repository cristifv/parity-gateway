package parity.gateway.agents;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.regex.Pattern;

/**
 * {@code command_id} from a desktop's frame id (parity-contracts CONVENTIONS.md, "command_id from
 * a desktop"): {@code "gw:" + base64url_nopad(SHA-256(L(tenant) tenant L(agent) agent L(id) id))},
 * {@code L} the UTF-8 length as 4 bytes big-endian. The same on any replica, never another agent's.
 */
public final class CommandIds {

    private static final Pattern UUID = Pattern.compile(
            "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$");

    private CommandIds() {}

    /** True for a lowercase canonical UUID: the only form a frame id may take. */
    public static boolean isUuid(String s) {
        return s != null && UUID.matcher(s).matches();
    }

    public static String derive(String tenantId, String agentId, String frameId) {
        if (!isUuid(frameId)) {
            throw new IllegalArgumentException("the frame id is not a UUID");
        }
        byte[][] parts = {bytes(tenantId), bytes(agentId), bytes(frameId)};
        ByteBuffer buf = ByteBuffer.allocate(12 + parts[0].length + parts[1].length + parts[2].length);
        for (byte[] p : parts) {
            buf.putInt(p.length).put(p);
        }
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(buf.array());
            return "gw:" + Base64.getUrlEncoder().withoutPadding().encodeToString(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is missing", e);
        }
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
