package parity.gateway.dev;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import parity.gateway.agents.creds.SipCredentials;
import parity.gateway.agents.ws.AgentPageHeaders;
import parity.gateway.lifecycle.ConfigurationRefusedException;

/** GW-003's confinement of the dev profile's static SIP passwords, and the page's CSP from the same setting. */
class DevSipPasswordsTest {

    private static final Clock CLOCK = Clock.fixed(Instant.ofEpochMilli(1_000_000), ZoneOffset.UTC);
    private static final List<String> LOCAL_WS = List.of("ws://127.0.0.1:5066");

    @TempDir
    Path dir;

    private Path file(String perms, String content) throws Exception {
        Path f = dir.resolve("sip-agents.properties");
        Files.writeString(f, content);
        Files.setPosixFilePermissions(f, PosixFilePermissions.fromString(perms));
        return f;
    }

    @Test
    void aListedAgentGetsItsPasswordAnUnlistedOneNothing() throws Exception {
        Path f = file("rw-------", "# comment\nacme/agent-a1=0123456789abcdef-pw\n");
        DevSipPasswords d = DevSipPasswords.load(f.toString(), "127.0.0.1", LOCAL_WS, Duration.ofMinutes(5), CLOCK);
        SipCredentials.Grant g = d.issue("acme", "agent-a1", "sip:agent-a1@acme.agents.localhost", "s");
        assertEquals("0123456789abcdef-pw", g.token());
        assertEquals(1_000_000 + 300_000, g.expiresAtMs());
        assertNull(d.issue("globex", "agent-a1", "sip:agent-a1@globex.agents.localhost", "s"), "another tenant's agent");
        assertNull(d.issue("acme", "agent-a2", "sip:agent-a2@acme.agents.localhost", "s"));
    }

    @Test
    void refusedOffLoopbackOrWithAReadableFile() throws Exception {
        Path f = file("rw-------", "acme/agent-a1=0123456789abcdef-pw\n");
        for (String bind : List.of("", "0.0.0.0", "10.0.0.5", "gateway.example")) {
            assertThrows(ConfigurationRefusedException.class,
                    () -> DevSipPasswords.load(f.toString(), bind, LOCAL_WS, Duration.ofMinutes(5), CLOCK), bind);
        }
        for (List<String> ws : List.of(List.<String>of(), List.of("wss://sip.acme.example"),
                List.of("ws://127.0.0.1:5066", "ws://10.0.0.5:5066"), List.of("ws://user@127.0.0.1:5066"))) {
            assertThrows(ConfigurationRefusedException.class,
                    () -> DevSipPasswords.load(f.toString(), "127.0.0.1", ws, Duration.ofMinutes(5), CLOCK), ws.toString());
        }
        Path readable = file("rw-r--r--", "acme/agent-a1=0123456789abcdef-pw\n");
        assertThrows(ConfigurationRefusedException.class,
                () -> DevSipPasswords.load(readable.toString(), "127.0.0.1", LOCAL_WS, Duration.ofMinutes(5), CLOCK));
        Path shortPassword = file("rw-------", "acme/agent-a1=short\n");
        assertThrows(ConfigurationRefusedException.class,
                () -> DevSipPasswords.load(shortPassword.toString(), "127.0.0.1", LOCAL_WS, Duration.ofMinutes(5), CLOCK));
    }

    @Test
    void theCspNamesExactlyTheSipWebSocketOrigins() {
        String csp = new AgentPageHeaders(List.of("wss://sip.acme.example:7443/ws", "ws://127.0.0.1:5066")).csp();
        assertTrue(csp.contains("connect-src 'self' wss://sip.acme.example:7443 ws://127.0.0.1:5066;"), csp);
        assertTrue(csp.contains("script-src 'self';") && csp.contains("frame-ancestors 'none'"), csp);
        assertThrows(ConfigurationRefusedException.class, () -> new AgentPageHeaders(List.of("https://x.example; script-src *")));
    }
}
