package parity.gateway.agents.creds;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * TURN REST API credentials (coturn {@code use-auth-secret}): username
 * {@code <expiry_s>:<tenant>.<agent>}, credential base64(HMAC-SHA1(secret, username)). Without a
 * secret file only the STUN URLs are given. Relay restrictions are a deploy gate (GW-011).
 */
public final class TurnCredentials {

    /** One ICE server entry for the desktop. */
    public record IceServer(List<String> urls, String username, String credential) {}

    /** The servers and when the credential expires. */
    public record Grant(List<IceServer> servers, long expiresAtMs) {}

    private final byte[] secret;
    private final List<String> urls;
    private final Duration ttl;
    private final Clock clock;

    public TurnCredentials(String secretFile, List<String> urls, Duration ttl, Clock clock) {
        this.urls = urls.stream().filter(u -> u != null && !u.isBlank()).toList();
        // GW-032: the secret is read only when there is a TURN URL to sign for; STUN alone (or no
        // ICE server at all, the local dev profile) needs none.
        boolean turn = this.urls.stream().anyMatch(u -> !u.startsWith("stun:"));
        try {
            this.secret = !turn || secretFile == null || secretFile.isBlank() ? null : Files.readAllBytes(Path.of(secretFile));
        } catch (java.io.IOException e) {
            throw new IllegalStateException("can't read the TURN secret file", e);
        }
        this.ttl = ttl;
        this.clock = clock;
    }

    public Grant issue(String tenantId, String agentId) {
        long expiresAtMs = clock.millis() + ttl.toMillis();
        List<String> stun = urls.stream().filter(u -> u.startsWith("stun:")).toList();
        List<String> turn = urls.stream().filter(u -> !u.startsWith("stun:")).toList();
        if (secret == null || turn.isEmpty()) {
            return new Grant(stun.isEmpty() ? List.of() : List.of(new IceServer(stun, null, null)), expiresAtMs);
        }
        String username = (expiresAtMs / 1000) + ":" + tenantId + "." + agentId;
        try {
            Mac mac = Mac.getInstance("HmacSHA1");
            mac.init(new SecretKeySpec(secret, "HmacSHA1"));
            String credential = Base64.getEncoder().encodeToString(mac.doFinal(username.getBytes(StandardCharsets.UTF_8)));
            List<IceServer> out = new java.util.ArrayList<>();
            if (!stun.isEmpty()) {
                out.add(new IceServer(stun, null, null));
            }
            out.add(new IceServer(turn, username, credential));
            return new Grant(List.copyOf(out), expiresAtMs);
        } catch (java.security.GeneralSecurityException e) {
            throw new IllegalStateException("HMAC-SHA1 failed", e);
        }
    }
}
