package parity.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import parity.gateway.agents.TestKeys;

/**
 * Step 0 on the packaged jar (profile {@code fake}: the fake RE and IE in-process): the healthy
 * file appears once started, {@code SIGTERM} removes it, drains, writes the drained file, exit 0.
 */
class ProcessLifecycleIT {

    @Test
    void startsHealthyAndDrainsOnSigterm() throws Exception {
        TestKeys keys = new TestKeys();
        Path dir = Files.createTempDirectory("gw-it");
        Path healthy = dir.resolve("healthy");
        Path drained = dir.resolve("drained");
        Path log = dir.resolve("out.log");
        Files.createDirectories(Path.of("target/it-logs"));
        ProcessBuilder pb = new ProcessBuilder(List.of("java", "-Djdk.tracePinnedThreads=short", "-jar",
                System.getProperty("gw.jar"), "--spring.profiles.active=fake", "--server.port=0",
                "--management.server.port=0", "--parity.gateway.fake.re-port=0", "--parity.gateway.fake.ie-port=0",
                "--parity.gateway.agents.re.port=1", "--parity.gateway.agents.ie.port=1",
                "--parity.gateway.lifecycle.healthy-file=" + healthy, "--parity.gateway.lifecycle.drained-file=" + drained,
                "--parity.gateway.lifecycle.drain-timeout=10s"))
                .redirectErrorStream(true).redirectOutput(log.toFile());
        pb.environment().put("FAKE_KEYS_DIR", keys.dir.toString());
        Process p = pb.start();
        try {
            long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            while (!Files.exists(healthy) && System.nanoTime() < end && p.isAlive()) {
                Thread.sleep(100);
            }
            assertTrue(Files.exists(healthy), "healthy file");
            new ProcessBuilder("kill", "-TERM", Long.toString(p.pid())).start().waitFor();
            assertTrue(p.waitFor(30, TimeUnit.SECONDS), "exited");
            assertEquals(0, p.exitValue());
            assertFalse(Files.exists(healthy));
            assertTrue(Files.exists(drained), "drained file");
        } finally {
            p.destroyForcibly();
            Files.copy(log, Path.of("target/it-logs/process-lifecycle.log"), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        }
    }
}
