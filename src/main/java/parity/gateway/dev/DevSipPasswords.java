package parity.gateway.dev;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import parity.gateway.agents.creds.SipCredentials;
import parity.gateway.lifecycle.ConfigurationRefusedException;

/**
 * Static SIP passwords for a local freeswitch registrar (PLAN question 5, GW-003): the {@code dev}
 * profile only, for the real-node test and a developer's machine, until opensips checks the SIP
 * JWT. Confined so it can't serve a real desktop: the gateway must listen on a loopback address,
 * every SIP WebSocket server must be on loopback, and the file (outside the jar, lines
 * {@code <tenant>/<agent>=<password>}) must be readable by its owner only. An agent without a line
 * gets no {@code sip_credentials} frame. Values are never logged.
 */
public final class DevSipPasswords implements SipCredentials {

    private static final Pattern LINE =
            Pattern.compile("^([a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)/([A-Za-z0-9][A-Za-z0-9._-]{0,63})=([\\x21-\\x7e]{16,256})$");
    private static final Set<PosixFilePermission> OWNER_ONLY =
            Set.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE);

    private final Map<String, String> passwords;
    private final Duration ttl;
    private final Clock clock;

    private DevSipPasswords(Map<String, String> passwords, Duration ttl, Clock clock) {
        this.passwords = Map.copyOf(passwords);
        this.ttl = ttl;
        this.clock = clock;
    }

    /**
     * Reads the file after the confinement checks; refuses (exit 2) when one fails.
     *
     * @param bindAddress {@code server.address}: must be a loopback address
     */
    public static DevSipPasswords load(String file, String bindAddress, List<String> wsServers, Duration ttl, Clock clock) {
        if (!loopback(bindAddress)) {
            throw new ConfigurationRefusedException("dev SIP passwords need server.address on loopback");
        }
        if (wsServers.isEmpty() || !wsServers.stream().allMatch(DevSipPasswords::loopbackWebSocket)) {
            throw new ConfigurationRefusedException("dev SIP passwords need every sip.ws-servers entry on loopback");
        }
        Path p = Path.of(file);
        Map<String, String> out = new HashMap<>();
        try {
            if (!Files.isRegularFile(p, LinkOption.NOFOLLOW_LINKS)
                    || !OWNER_ONLY.containsAll(Files.getPosixFilePermissions(p, LinkOption.NOFOLLOW_LINKS))) {
                throw new ConfigurationRefusedException("the dev SIP passwords file must be a regular file readable by its owner only");
            }
            for (String line : Files.readAllLines(p, StandardCharsets.UTF_8)) {
                if (line.isBlank() || line.startsWith("#")) {
                    continue;
                }
                var m = LINE.matcher(line.strip());
                if (!m.matches()) {
                    throw new ConfigurationRefusedException("a dev SIP passwords line is not <tenant>/<agent>=<16 or more printable characters>");
                }
                out.put(m.group(1) + "/" + m.group(2), m.group(3));
            }
        } catch (IOException e) {
            throw new ConfigurationRefusedException("can't read the dev SIP passwords file");
        }
        return new DevSipPasswords(out, ttl, clock);
    }

    public int size() {
        return passwords.size();
    }

    @Override
    public Grant issue(String tenantId, String agentId, String sipUri, String sessionId) {
        String password = passwords.get(tenantId + "/" + agentId);
        return password == null ? null : new Grant(password, clock.millis() + ttl.toMillis());
    }

    static boolean loopbackWebSocket(String url) {
        try {
            URI u = new URI(url);
            return ("ws".equals(u.getScheme()) || "wss".equals(u.getScheme())) && u.getHost() != null
                    && u.getUserInfo() == null && loopback(u.getHost());
        } catch (java.net.URISyntaxException e) {
            return false;
        }
    }

    /** A literal loopback address or {@code localhost}; never a DNS lookup of another name. */
    static boolean loopback(String host) {
        if (host == null || host.isBlank()) {
            return false;
        }
        String h = host.startsWith("[") && host.endsWith("]") ? host.substring(1, host.length() - 1) : host;
        if ("localhost".equals(h)) {
            return true;
        }
        if (!h.matches("^[0-9.]+$") && !h.matches("^[0-9a-fA-F:]+$")) {
            return false;
        }
        try {
            return InetAddress.getByName(h).isLoopbackAddress();
        } catch (UnknownHostException e) {
            return false;
        }
    }
}
