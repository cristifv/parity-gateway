package parity.gateway.agents.creds;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** GW-032: the TURN secret is read only when a TURN URL needs it. */
class TurnCredentialsTest {

    @Test
    void withoutTurnUrlsNoSecretFileIsNeeded(@TempDir Path dir) {
        String missing = dir.resolve("absent.secret").toString();
        TurnCredentials none = new TurnCredentials(missing, List.of(), Duration.ofHours(1), Clock.systemUTC());
        assertTrue(none.issue("acme", "agent-a1").servers().isEmpty());
        // An empty flag value (compose's `--...turn.urls=`) is no URL either.
        TurnCredentials blank = new TurnCredentials(missing, List.of(""), Duration.ofHours(1), Clock.systemUTC());
        assertTrue(blank.issue("acme", "agent-a1").servers().isEmpty());
        TurnCredentials stun = new TurnCredentials(missing, List.of("stun:stun.example:3478"), Duration.ofHours(1),
                Clock.systemUTC());
        var s = stun.issue("acme", "agent-a1").servers();
        assertEquals(1, s.size());
        assertNull(s.get(0).username());
    }

    @Test
    void aTurnUrlNeedsItsSecretAndSignsWithIt(@TempDir Path dir) throws Exception {
        List<String> urls = List.of("stun:turn.example:3478", "turn:turn.example:3478?transport=udp");
        String missing = dir.resolve("absent.secret").toString();
        assertThrows(IllegalStateException.class,
                () -> new TurnCredentials(missing, urls, Duration.ofHours(1), Clock.systemUTC()));
        Path secret = dir.resolve("turn.secret");
        Files.writeString(secret, "not-a-real-secret-only-for-this-test");
        var servers = new TurnCredentials(secret.toString(), urls, Duration.ofHours(1), Clock.systemUTC())
                .issue("acme", "agent-a1").servers();
        assertEquals(2, servers.size());
        assertTrue(servers.get(1).username().endsWith(":acme.agent-a1"));
        assertNotNull(servers.get(1).credential());
    }
}
