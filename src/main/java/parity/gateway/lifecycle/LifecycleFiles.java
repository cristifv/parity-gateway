package parity.gateway.lifecycle;

import jakarta.annotation.PostConstruct;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.availability.AvailabilityChangeEvent;
import org.springframework.boot.availability.ReadinessState;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import parity.gateway.config.GatewayProperties;

/**
 * Startup, readiness and the drain, told to the orchestrator with two empty files (ARCH §7.3):
 * <ul>
 *   <li><b>the healthy file</b>, created the first time every readiness check passes after the
 *       application is ready, and removed when the drain starts. It means "started and not
 *       draining" (interaction-engine's ruling F3): a check that fails later turns
 *       {@code /readyz} down and is logged, but leaves the file;</li>
 *   <li><b>the drained file</b>, created only when the drain finished within its timeout.</li>
 * </ul>
 * Both are removed at start.
 *
 * <p>A healthy file that can't be written (a full or read-only {@code /tmp}) is counted
 * ({@code parity_gateway_healthy_file_errors_total}), logged, and tried again with a
 * backoff from the poll interval up to 5 s; the gate keeps running.
 *
 * <p>The drain (the context's close, on the first {@code SIGTERM} or {@code SIGINT}; see
 * {@link ShutdownSignals}): remove the healthy file and refuse readiness; wait the
 * deregistration delay; drain each {@link Drainable}; create the drained file. Past the drain
 * timeout there is no drained file and the process exits 1.
 */
@Component
public class LifecycleFiles implements SmartLifecycle {

    /** How the drain ended. */
    public enum Outcome { RUNNING, DRAINED, TIMED_OUT }

    private static final Logger log = LoggerFactory.getLogger(LifecycleFiles.class);
    private static final Duration MAX_RETRY = Duration.ofSeconds(5);

    private final GatewayProperties properties;
    private final ReadinessChecks checks;
    private final List<Drainable> drainables;
    private final ApplicationEventPublisher events;
    /** Writing the healthy file and starting the drain exclude each other. */
    private final ReentrantLock gateLock = new ReentrantLock();
    private volatile boolean running;
    private volatile boolean draining;
    private volatile boolean healthy;
    private volatile Map<String, String> failures = Map.of("gate", "not evaluated yet");
    private volatile Outcome outcome = Outcome.RUNNING;
    private volatile Thread gate;
    private final AtomicLong healthyFileErrors = new AtomicLong();
    /** The gate's next attempt at the healthy file after a failure (System.nanoTime). */
    private long retryAt = System.nanoTime();
    private Duration retryDelay;

    public LifecycleFiles(GatewayProperties properties, ReadinessChecks checks,
            ObjectProvider<Drainable> drainables, ApplicationEventPublisher events) {
        this.properties = properties;
        this.checks = checks;
        this.drainables = drainables.orderedStream().toList();
        this.events = events;
    }

    /** Files left by an earlier run would tell the orchestrator the wrong thing. */
    @PostConstruct
    void removeStaleFiles() throws IOException {
        for (Path file : new Path[] {healthyFile(), drainedFile()}) {
            if (Files.deleteIfExists(file)) {
                log.atInfo().addKeyValue("event", "lifecycle.stale_file_removed").addKeyValue("file", file.toString())
                        .log("removed a file left by an earlier run");
            }
        }
    }

    /** The gate starts once the application is ready: never a healthy file before that. */
    @EventListener(ApplicationReadyEvent.class)
    public void startGate() {
        log.atInfo().addKeyValue("event", "readiness.gate_started").addKeyValue("checks", checks.names())
                .log("started; waiting for the readiness checks");
        gate = Thread.ofVirtual().name("readiness-gate").start(this::gateLoop);
    }

    private void gateLoop() {
        Duration poll = properties.readiness().pollInterval();
        while (!draining) {
            Map<String, String> now = checks.evaluate();
            if (!now.equals(failures)) {
                if (now.isEmpty()) {
                    log.atInfo().addKeyValue("event", "readiness.passed").log("readiness checks pass");
                } else {
                    log.atWarn().addKeyValue("event", "service.not_ready").addKeyValue("failing", now).log("readiness checks fail");
                }
            }
            failures = Map.copyOf(now);
            if (now.isEmpty() && !healthy && System.nanoTime() - retryAt >= 0) {
                writeHealthyUnlessDraining();
            }
            try {
                Thread.sleep(poll);
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    private void writeHealthyUnlessDraining() {
        gateLock.lock();
        try {
            if (draining || ShutdownSignals.requested()) {
                return;
            }
            touch(healthyFile());
            healthy = true;
            retryDelay = null;
            log.atInfo().addKeyValue("event", "service.ready").addKeyValue("file", healthyFile().toString()).log("healthy");
        } catch (UncheckedIOException e) {
            healthyFileErrors.incrementAndGet();
            Duration poll = properties.readiness().pollInterval();
            retryDelay = retryDelay == null ? poll : retryDelay.multipliedBy(2);
            if (retryDelay.compareTo(MAX_RETRY) > 0) {
                retryDelay = MAX_RETRY;
            }
            retryAt = System.nanoTime() + retryDelay.toNanos();
            log.atError().addKeyValue("event", "lifecycle.file_failed").addKeyValue("file", healthyFile().toString())
                    .addKeyValue("error_kind", e.getCause().getClass().getName())
                    .addKeyValue("retry_ms", retryDelay.toMillis())
                    .log("can't write the healthy file; trying again");
        } finally {
            gateLock.unlock();
        }
    }

    /** Healthy-file writes that failed. */
    public long healthyFileErrors() {
        return healthyFileErrors.get();
    }

    /** True while every check passes and no drain has started: {@code /readyz}. */
    public boolean ready() {
        return !draining && failures.isEmpty();
    }

    public Map<String, String> failures() {
        return draining ? Map.of("drain", "draining") : failures;
    }

    public Outcome outcome() {
        return outcome;
    }

    @Override
    public void start() {
        running = true;
    }

    @Override
    public void stop() {
        long startedAt = System.nanoTime();
        long deadline = startedAt + properties.lifecycle().drainTimeout().toNanos();
        gateLock.lock();
        try {
            draining = true;
            Files.deleteIfExists(healthyFile());
            healthy = false;
        } catch (IOException e) {
            log.atError().addKeyValue("event", "lifecycle.file_failed").addKeyValue("file", healthyFile().toString())
                    .addKeyValue("error_kind", e.getClass().getName()).log("can't remove the healthy file");
        } finally {
            gateLock.unlock();
        }
        Thread g = gate;
        if (g != null) {
            g.interrupt();
        }
        AvailabilityChangeEvent.publish(events, this, ReadinessState.REFUSING_TRAFFIC);
        log.atInfo().addKeyValue("event", "service.draining").addKeyValue("file", healthyFile().toString())
                .log("draining: removed the healthy file");
        try {
            sleepUntil(Math.min(deadline, startedAt + properties.lifecycle().deregistrationDelay().toNanos()));
            for (Drainable work : drainables) {
                Duration left = Duration.ofNanos(Math.max(0, deadline - System.nanoTime()));
                if (!work.drain(left) || System.nanoTime() > deadline) {
                    timedOut(work.getClass().getSimpleName());
                    return;
                }
            }
            try {
                touch(drainedFile());
            } catch (UncheckedIOException e) {
                outcome = Outcome.TIMED_OUT;
                log.atError().addKeyValue("event", "lifecycle.file_failed").addKeyValue("file", drainedFile().toString())
                        .addKeyValue("error_kind", e.getCause().getClass().getName())
                        .log("drained, but the drained file can't be written");
                return;
            }
            outcome = Outcome.DRAINED;
            log.atInfo().addKeyValue("event", "drain.completed").addKeyValue("file", drainedFile().toString())
                    .addKeyValue("duration_ms", (System.nanoTime() - startedAt) / 1_000_000).log("drained");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            timedOut("interrupted");
        } finally {
            running = false;
        }
    }

    private void timedOut(String what) {
        outcome = Outcome.TIMED_OUT;
        log.atError().addKeyValue("event", "drain.timed_out").addKeyValue("waiting_for", what)
                .addKeyValue("drain_timeout_ms", properties.lifecycle().drainTimeout().toMillis())
                .log("not drained in time; no drained file");
    }

    private static void sleepUntil(long nanoTime) throws InterruptedException {
        long left = nanoTime - System.nanoTime();
        if (left > 0) {
            Thread.sleep(Duration.ofNanos(left));
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    /** First to stop: before the web server and anything else. */
    @Override
    public int getPhase() {
        return Integer.MAX_VALUE;
    }

    private Path healthyFile() {
        return Path.of(properties.lifecycle().healthyFile());
    }

    private Path drainedFile() {
        return Path.of(properties.lifecycle().drainedFile());
    }

    private static void touch(Path file) {
        try {
            Files.write(file, new byte[0]);
        } catch (IOException e) {
            throw new UncheckedIOException("can't create " + file, e);
        }
    }
}
