package parity.gateway.obs;

import ch.qos.logback.classic.AsyncAppender;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.event.KeyValuePair;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Logback's {@link AsyncAppender} with {@code neverBlock}: a full queue drops the record instead
 * of making the caller wait (a session's executor must never block on stdout). On the caller's
 * thread, before the queue, it does what log-record.v1 asks of the moment a record is made:
 * <ul>
 *   <li>{@code seq}: the process's next number, so a dropped record leaves a gap;</li>
 *   <li>the caller data ({@code code}: file, line, function) at {@code warn} and above only;</li>
 *   <li>dedup: an event whose catalogue entry has {@code dedup} (the {@code catalogue} resource,
 *       plus the common {@code dependency.unavailable}) is written at most once per window and
 *       key (event, error code, error kind, the entry's key fields); the ones held back are
 *       counted, and the next record of that key carries {@code repeat_count} (and
 *       {@code repeat_since}, the first one's time). When the window ends with none, the last
 *       held record is written then, with the count.</li>
 * </ul>
 * Drops are counted here, exported as {@code parity_gateway_log_lines_dropped_total} and reported
 * as {@code log.dropped} at most every 10 s. The count is taken when the queue is seen full, so
 * it can be off by the few records raced in parallel.
 */
public class CountingAsyncAppender extends AsyncAppender {

    private static final AtomicLong DROPPED = new AtomicLong();
    private static final AtomicLong REPORTED = new AtomicLong();
    private static final long REPORT_EVERY_MS = 10_000;
    private static final int MAX_KEYS = 4096;
    /** parity-contracts' logging/events.common.v1.json. */
    private static final Map<String, Dedup> COMMON = Map.of(
            "dependency.unavailable", new Dedup(10_000, List.of("attrs.dependency")));

    private record Dedup(long windowMs, List<String> key) {}

    /** One key's window; changed only inside {@link ConcurrentHashMap#compute}. */
    private static final class Window {
        final long start;
        final long windowMs;
        long held;
        long firstHeld;
        ILoggingEvent last;

        Window(long start, long windowMs) {
            this.start = start;
            this.windowMs = windowMs;
        }
    }

    private String catalogue = "";
    private Map<String, Dedup> dedup = COMMON;
    private final ConcurrentHashMap<String, Window> windows = new ConcurrentHashMap<>();
    private ScheduledExecutorService reporter;
    private long lastReportMs;

    public static long dropped() {
        return DROPPED.get();
    }

    /** The classpath resource of the service's catalogue ({@code logging/events.<service>.v1.json}). */
    public void setCatalogue(String catalogue) {
        this.catalogue = catalogue;
    }

    @Override
    public void start() {
        dedup = load(catalogue);
        super.start();
        reporter = Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().name("log-reporter").daemon(true).factory());
        var unused = reporter.scheduleWithFixedDelay(this::tick, 1, 1, TimeUnit.SECONDS);
    }

    @Override
    public void stop() {
        if (reporter != null) {
            reporter.shutdownNow();
        }
        flush(Long.MAX_VALUE);
        super.stop();
    }

    private Map<String, Dedup> load(String resource) {
        Map<String, Dedup> m = new HashMap<>(COMMON);
        if (resource == null || resource.isBlank()) {
            return m;
        }
        try (InputStream in = CountingAsyncAppender.class.getClassLoader().getResourceAsStream(resource)) {
            if (in == null) {
                addWarn("no log catalogue on the classpath at " + resource + ": no dedup");
                return m;
            }
            JsonNode events = JsonMapper.builder().build().readTree(in).path("events");
            for (var e : events.properties()) {
                JsonNode d = e.getValue().path("dedup");
                if (d.isObject()) {
                    List<String> key = new ArrayList<>();
                    d.path("key").forEach(k -> key.add(k.asString()));
                    m.put(e.getKey(), new Dedup(d.path("window_ms").asLong(1000), List.copyOf(key)));
                }
            }
        } catch (java.io.IOException | RuntimeException e) {
            addWarn("the log catalogue " + resource + " can't be read: no dedup", e);
        }
        return Map.copyOf(m);
    }

    @Override
    protected void preprocess(ILoggingEvent event) {
        super.preprocess(event);
        if (event.getLevel() != null && event.getLevel().isGreaterOrEqual(Level.WARN)) {
            var unused = event.getCallerData();
        }
    }

    @Override
    protected void append(ILoggingEvent event) {
        if (!isStarted() || held(event)) {
            return;
        }
        write(event);
    }

    private void write(ILoggingEvent event) {
        if (event instanceof LoggingEvent e) {
            e.addKeyValuePair(new KeyValuePair(JsonLogFormatter.SEQ, JsonLogFormatter.nextSeq()));
        }
        if (getRemainingCapacity() == 0) {
            DROPPED.incrementAndGet();
        }
        super.append(event);
    }

    /** True when the record is held back by its event's dedup window. */
    private boolean held(ILoggingEvent event) {
        String name = value(event, JsonLogFormatter.EVENT);
        Dedup spec = name == null ? null : dedup.get(name);
        if (spec == null || !(event instanceof LoggingEvent e)) {
            return false;
        }
        StringBuilder k = new StringBuilder(name).append('|').append(value(event, JsonLogFormatter.ERROR_CODE))
                .append('|').append(event.getThrowableProxy() != null ? event.getThrowableProxy().getClassName()
                        : value(event, JsonLogFormatter.ERROR_KIND));
        for (String field : spec.key()) {
            k.append('|').append(value(event, field.startsWith("attrs.") ? field.substring(6) : field));
        }
        String key = k.toString();
        if (!windows.containsKey(key) && windows.size() >= MAX_KEYS) {
            return false;
        }
        long now = event.getTimeStamp();
        boolean[] hold = {false};
        long[] carried = {0, 0};
        windows.compute(key, (x, w) -> {
            if (w == null || now - w.start >= w.windowMs) {
                if (w != null && w.held > 0) {
                    carried[0] = w.held;
                    carried[1] = w.firstHeld;
                }
                return new Window(now, spec.windowMs());
            }
            w.held++;
            if (w.held == 1) {
                w.firstHeld = now;
            }
            w.last = event;
            hold[0] = true;
            return w;
        });
        if (hold[0]) {
            preprocess(event);
            return true;
        }
        if (carried[0] > 0) {
            e.addKeyValuePair(new KeyValuePair(JsonLogFormatter.REPEAT_COUNT, carried[0]));
            e.addKeyValuePair(new KeyValuePair(JsonLogFormatter.REPEAT_SINCE, carried[1]));
        }
        return false;
    }

    private static String value(ILoggingEvent event, String key) {
        List<KeyValuePair> pairs = event.getKeyValuePairs();
        if (pairs != null) {
            for (KeyValuePair p : pairs) {
                if (key.equals(p.key)) {
                    return p.value == null ? null : String.valueOf(p.value);
                }
            }
        }
        Map<String, String> mdc = event.getMDCPropertyMap();
        return mdc == null ? null : mdc.get(key);
    }

    private void tick() {
        try {
            long now = System.currentTimeMillis();
            flush(now);
            long dropped = DROPPED.get();
            long reported = REPORTED.get();
            if (dropped > reported && now - lastReportMs >= REPORT_EVERY_MS && REPORTED.compareAndSet(reported, dropped)) {
                lastReportMs = now;
                ((LoggerContext) getContext()).getLogger(CountingAsyncAppender.class).atWarn()
                        .addKeyValue(JsonLogFormatter.EVENT, "log.dropped").addKeyValue("dropped", dropped - reported)
                        .log("log records dropped: the log queue was full");
            }
        } catch (RuntimeException e) {
            addWarn("the log reporter failed", e);
        }
    }

    /** Ends the windows past their time; a window that held records writes the last one, with the count. */
    private void flush(long now) {
        for (String key : List.copyOf(windows.keySet())) {
            ILoggingEvent[] last = {null};
            long[] count = {0, 0};
            windows.computeIfPresent(key, (k, w) -> {
                if (now - w.start < w.windowMs) {
                    return w;
                }
                last[0] = w.last;
                count[0] = w.held;
                count[1] = w.firstHeld;
                return null;
            });
            if (last[0] instanceof LoggingEvent e && count[0] > 0 && isStarted()) {
                e.addKeyValuePair(new KeyValuePair(JsonLogFormatter.REPEAT_COUNT, count[0]));
                e.addKeyValuePair(new KeyValuePair(JsonLogFormatter.REPEAT_SINCE, count[1]));
                write(e);
            }
        }
    }
}
