package parity.gateway.obs;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.classic.spi.StackTraceElementProxy;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import org.slf4j.event.KeyValuePair;

/**
 * log-record.v1 (parity-contracts CONVENTIONS "Logging (log-record.v1)"): one JSON object per
 * line, at most {@value #MAX_LINE} bytes.
 * <ul>
 *   <li>the envelope: {@code ts} (RFC 3339, UTC, ms), lowercase {@code level}, {@code service},
 *       {@code version}, {@code instance}, {@code incarnation} (this process's start), {@code seq}
 *       (stamped by {@link CountingAsyncAppender} when the record is made, so a dropped record
 *       leaves a gap), {@code event}, {@code msg}, {@code thread}, {@code component} (the logger);</li>
 *   <li>the event's name is the key-value pair {@value #EVENT}
 *       ({@code log.atInfo().addKeyValue(EVENT, name)}), a name from
 *       {@code logging/events.<service>.v1.json}; a record of this service without one is
 *       {@code log.unclassified} (which no catalogue lists: CI refuses it), a library's record (no
 *       key-value pairs, whatever its logger's name) is {@code log.external};</li>
 *   <li>correlation and flow fields ({@code tenant_id}, {@code xsid}, {@code agent_id},
 *       {@code request_id}, {@code op}, {@code outcome}, ...) go to the top level when their value
 *       has the schema's form, everything else under {@code attrs}, masked by name
 *       ({@link Pii#maskField}) and free text scrubbed ({@link Pii#scrub}). An {@code interaction_id}
 *       is written as the {@code xsid} (the same id); one that isn't a UUIDv7 stays
 *       {@code interaction_id};</li>
 *   <li>{@code error}: {@code code} (the key-value {@value #ERROR_CODE}), {@code kind} (the
 *       exception's class, or {@value #ERROR_KIND}), {@code message}, and {@code stack} at
 *       {@code error} only (30 frames per throwable, causes as {@code caused by:} lines, 8192
 *       characters);</li>
 *   <li>{@code code}: {@code file}, {@code line}, {@code func} of this service's caller, at
 *       {@code warn} and above (where the appender took the caller data);</li>
 *   <li>{@code repeat_count}/{@code repeat_since}: set by the appender's dedup.</li>
 * </ul>
 * A line that would be longer than {@value #MAX_LINE} bytes is shortened (attrs strings, the
 * stack, then attrs), never split.
 */
public final class JsonLogFormatter {

    /** The key-value pair that names a record's event. */
    public static final String EVENT = "event";
    /** A contract error code ({@code error.code}). */
    public static final String ERROR_CODE = "error_code";
    /** An error's type when there is no exception ({@code error.kind}). */
    public static final String ERROR_KIND = "error_kind";
    /** An error's text when there is no exception ({@code error.message}). */
    public static final String ERROR_MESSAGE = "error_message";
    /** Internal key-value pairs the appender adds; never written as attributes. */
    static final String SEQ = "log.seq";
    static final String REPEAT_COUNT = "log.repeat_count";
    static final String REPEAT_SINCE = "log.repeat_since";

    /** This service's name, when nothing configured it. */
    private static final String SERVICE = "parity-gateway";
    static final int MAX_LINE = 16_000;
    private static final int MAX_MSG = 256;
    private static final int MAX_ERROR_MESSAGE = 1024;
    private static final int MAX_STACK = 8192;
    private static final int MAX_FRAMES = 30;
    private static final int MAX_ATTRS = 32;
    private static final String TRUNCATED = "...[truncated]";

    /** This process's start: {@code seq} restarts at 1 with it. */
    static final String INCARNATION = Long.toString(System.currentTimeMillis(), 36) + "-"
            + Integer.toHexString(ThreadLocalRandom.current().nextInt(0x10000, 0x100000));
    private static final AtomicLong SEQS = new AtomicLong();

    static final DateTimeFormatter TS =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC);

    private static final Pattern UUID =
            Pattern.compile("^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$");
    private static final Pattern UUID7 =
            Pattern.compile("^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$");
    private static final Pattern OPAQUE = Pattern.compile("^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$");
    private static final Pattern COMMAND = Pattern.compile("^[A-Za-z0-9._:-]{1,200}$");
    private static final Pattern PLACE = Pattern.compile("^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$");
    private static final Pattern STATE = Pattern.compile("^[A-Za-z][A-Za-z0-9_]{0,63}$");
    private static final Pattern TENANT = Pattern.compile("^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$");
    private static final Pattern TRACE = Pattern.compile("^(?!0{32}$)[0-9a-f]{32}$");
    private static final Pattern SPAN = Pattern.compile("^(?!0{16}$)[0-9a-f]{16}$");
    private static final Pattern TRACEPARENT = Pattern.compile("^[0-9a-f]{2}-([0-9a-f]{32})-([0-9a-f]{16})-[0-9a-f]{2}$");
    private static final Pattern QUEUE = Pattern.compile("^[A-Za-z0-9_.-]{1,64}$");
    private static final Pattern COMPONENT = Pattern.compile("^[A-Za-z_][A-Za-z0-9_.:/$-]{0,127}$");
    private static final Pattern OP = Pattern.compile("^[A-Za-z0-9_./:{}* -]{1,128}$");
    private static final Pattern CAUSE = Pattern.compile(
            "^(event|rpc|cmd|timer|sip|http|kafka|ws|signal|operator|config|startup|internal):[A-Za-z0-9._:/@{}* -]{1,190}$");
    private static final Pattern OUTCOME = Pattern.compile("^(ok|refused|error|timeout|retry)$");
    private static final Pattern CODE = Pattern.compile("^[a-z][a-z0-9_]{0,63}$");
    private static final Pattern KIND = Pattern.compile("^[A-Za-z_][A-Za-z0-9_.$:<>]{0,127}$");
    private static final Pattern ATTR = Pattern.compile("^[a-z][a-z0-9_]{0,63}$");
    private static final Pattern EVENT_NAME = Pattern.compile("^(?=.{1,64}$)[a-z][a-z0-9_]*(\\.[a-z][a-z0-9_]*)+$");
    private static final Pattern VERSION = Pattern.compile("^[0-9A-Za-z][0-9A-Za-z.+_-]{0,63}$");

    /** The string fields log-record.v1 has at the top level, in the order they are written. */
    private static final Map<String, Pattern> TOP = top();
    /** The numeric ones: non-negative integers (epochs), a duration, an attempt. */
    private static final Set<String> COUNTERS = Set.of("leader_epoch", "owner_epoch");

    private static Map<String, Pattern> top() {
        Map<String, Pattern> m = new LinkedHashMap<>();
        m.put("trace_id", TRACE);
        m.put("span_id", SPAN);
        m.put("tenant_id", TENANT);
        m.put("xsid", UUID7);
        m.put("parent_xsid", UUID7);
        m.put("call_id", UUID);
        m.put("session_id", OPAQUE);
        m.put("leg_id", UUID);
        m.put("agent_id", OPAQUE);
        m.put("queue_id", QUEUE);
        m.put("entry_id", UUID);
        m.put("reservation_id", UUID);
        m.put("request_id", COMMAND);
        m.put("command_id", COMMAND);
        m.put("event_id", UUID);
        m.put("bin", PLACE);
        m.put("zone", PLACE);
        m.put("cell", PLACE);
        m.put("node", OPAQUE);
        m.put("op", OP);
        m.put("from_state", STATE);
        m.put("to_state", STATE);
        m.put("cause", CAUSE);
        m.put("outcome", OUTCOME);
        return java.util.Collections.unmodifiableMap(m);
    }

    /** Who writes: the service, its build and this process. */
    public record Identity(String service, String version, String instance, String zone, String bin) {}

    private final String service;
    private final String version;
    private final String instance;
    private final String zone;
    private final String bin;
    /** This service's packages ({@code parity.gateway}): its records must name an event. */
    private final String own;

    public JsonLogFormatter(Identity id) {
        this.service = id.service();
        this.version = id.version() != null && VERSION.matcher(id.version()).matches() ? id.version() : "unknown";
        this.instance = instanceName(id.instance());
        this.zone = place(id.zone());
        this.bin = place(id.bin());
        String pkg = JsonLogFormatter.class.getPackageName();
        this.own = pkg.substring(0, pkg.lastIndexOf('.') + 1);
    }

    /** The identity {@link #direct} records use (the encoder's, once logging is configured). */
    static void remember(Identity id) {
        current = id;
    }

    /** The identity of the last formatter made: for {@link #direct} records. */
    private static volatile Identity current;

    /**
     * A record written without logback: a signal before logging is set up, or one that halts the
     * JVM before the async appender could write. One line, with its newline.
     */
    public static String direct(String level, String event, String msg, Map<String, Object> attrs) {
        Identity id = current;
        Map<String, Object> line = new LinkedHashMap<>();
        line.put("ts", TS.format(Instant.now()));
        line.put("level", level);
        line.put("service", id != null ? id.service() : SERVICE);
        line.put("version", id != null && id.version() != null && VERSION.matcher(id.version()).matches() ? id.version() : "unknown");
        line.put("instance", instanceName(id != null ? id.instance() : null));
        line.put("incarnation", INCARNATION);
        line.put("seq", nextSeq());
        line.put("event", event);
        line.put("msg", msg);
        Map<String, Object> a = new LinkedHashMap<>();
        attrs.forEach((k, v) -> attr(a, k, v));
        if (!a.isEmpty()) {
            line.put("attrs", a);
        }
        return fit(line);
    }

    /** The next {@code seq} of this process. */
    static long nextSeq() {
        return SEQS.incrementAndGet();
    }

    private static String instanceName(String configured) {
        String name = configured == null || configured.isBlank() || "unknown".equals(configured)
                ? System.getenv("HOSTNAME") : configured;
        if (name == null || name.isBlank()) {
            return "unknown";
        }
        String clean = name.replaceAll("[^A-Za-z0-9._:-]", "-");
        clean = clean.length() > 128 ? clean.substring(0, 128) : clean;
        return OPAQUE.matcher(clean).matches() ? clean : "unknown";
    }

    private static String place(String configured) {
        return configured == null || "unknown".equals(configured) || "none".equals(configured)
                || !PLACE.matcher(configured).matches() ? null : configured;
    }

    /** One record, with its newline. */
    public String format(ILoggingEvent e) {
        Map<String, Object> kv = new LinkedHashMap<>();
        Map<String, String> mdc = e.getMDCPropertyMap();
        if (mdc != null) {
            kv.putAll(mdc);
        }
        List<KeyValuePair> pairs = e.getKeyValuePairs();
        // A call site of this service names its event with key-value pairs; a library's record (even one
        // under this service's class names: Spring's startup lines, its filters' debug lines) has none.
        boolean structured = false;
        if (pairs != null) {
            for (KeyValuePair p : pairs) {
                if (p != null && p.key != null) {
                    kv.put(p.key, p.value);
                    structured |= !p.key.startsWith("log.");
                }
            }
        }
        Object named = kv.remove(EVENT);
        Object seq = kv.remove(SEQ);
        Object repeatCount = kv.remove(REPEAT_COUNT);
        Object repeatSince = kv.remove(REPEAT_SINCE);
        Object errorCode = kv.remove(ERROR_CODE);
        Object errorKind = kv.remove(ERROR_KIND);
        Object errorMessage = kv.remove(ERROR_MESSAGE);

        String logger = e.getLoggerName() == null ? "" : e.getLoggerName();
        boolean ours = logger.startsWith(own) && structured;
        String event = named instanceof String s && EVENT_NAME.matcher(s).matches() ? s
                : ours ? "log.unclassified" : "log.external";

        Map<String, Object> line = new LinkedHashMap<>();
        line.put("ts", TS.format(Instant.ofEpochMilli(e.getTimeStamp())));
        line.put("level", level(e.getLevel(), event));
        line.put("service", service);
        line.put("version", version);
        line.put("instance", instance);
        line.put("incarnation", INCARNATION);
        line.put("seq", seq instanceof Long n ? n : nextSeq());
        line.put("event", event);
        line.put("msg", message(e));
        if (e.getThreadName() != null && !e.getThreadName().isEmpty()) {
            line.put("thread", clip(Pii.scrub(e.getThreadName()), 64));
        }
        if (COMPONENT.matcher(logger).matches()) {
            line.put("component", logger);
        }

        Map<String, Object> top = new LinkedHashMap<>();
        Map<String, Object> attrs = new LinkedHashMap<>();
        if (zone != null) {
            top.put("zone", zone);
        }
        if (bin != null) {
            top.put("bin", bin);
        }
        for (Map.Entry<String, Object> f : kv.entrySet()) {
            Object v = ProtoLogGuard.safe(f.getValue());
            if (v == null) {
                continue;
            }
            if (!lift(f.getKey(), v, top)) {
                attr(attrs, f.getKey(), v);
            }
        }
        // The schema's order: correlation, then flow (as TOP lists them), then numbers.
        for (String name : TOP.keySet()) {
            Object v = top.remove(name);
            if (v != null) {
                line.put(name, v);
            }
        }
        top.forEach(line::put);

        Map<String, Object> error = error(e, errorCode, errorKind, errorMessage, attrs);
        if (!error.isEmpty()) {
            line.put("error", error);
        }
        Map<String, Object> code = code(e);
        if (code != null) {
            line.put("code", code);
        }
        if (repeatCount instanceof Long n && n > 0) {
            line.put("repeat_count", n);
            if (repeatSince instanceof Long since) {
                line.put("repeat_since", TS.format(Instant.ofEpochMilli(since)));
            }
        }
        if (!logger.isEmpty() && !ours) {
            attrs.putIfAbsent("logger", clip(logger, 128));
        }
        if (!attrs.isEmpty()) {
            line.put("attrs", attrs);
        }
        return fit(line);
    }

    private static String level(Level level, String event) {
        if ("service.panic".equals(event)) {
            return "fatal";
        }
        return level == null ? "info" : level.toString().toLowerCase(Locale.ROOT);
    }

    /** The message, with any protobuf argument replaced by its masked fields (R1-5), scrubbed. */
    private static String message(ILoggingEvent event) {
        String text;
        Object[] args = event.getArgumentArray();
        if (args == null || java.util.Arrays.stream(args).noneMatch(a -> a instanceof com.google.protobuf.MessageOrBuilder)) {
            text = event.getFormattedMessage();
        } else {
            Object[] safe = java.util.Arrays.stream(args).map(ProtoLogGuard::safe).toArray();
            text = org.slf4j.helpers.MessageFormatter.arrayFormat(event.getMessage(), safe).getMessage();
        }
        if (text == null || text.isBlank()) {
            return "-";
        }
        return clip(Pii.scrub(text.strip()), MAX_MSG);
    }

    /** Puts a correlation or flow field at the top level when its value has the schema's form. */
    private static boolean lift(String name, Object v, Map<String, Object> top) {
        if ("traceparent".equals(name)) {
            var m = TRACEPARENT.matcher(String.valueOf(v));
            if (m.matches() && TRACE.matcher(m.group(1)).matches() && SPAN.matcher(m.group(2)).matches()) {
                top.put("trace_id", m.group(1));
                top.put("span_id", m.group(2));
                return true;
            }
            return false;
        }
        if ("xsid".equals(name) || "interaction_id".equals(name)) {
            String id = String.valueOf(v).toLowerCase(Locale.ROOT);
            if (UUID7.matcher(id).matches()) {
                if (top.containsKey("xsid") && !id.equals(top.get("xsid"))) {
                    return false;
                }
                top.put("xsid", id);
                return true;
            }
            if (UUID.matcher(id).matches() && !top.containsKey("xsid")) {
                top.put("interaction_id", id);
                return true;
            }
            return false;
        }
        if (COUNTERS.contains(name)) {
            if (v instanceof Number n && n.longValue() >= 0 && !(v instanceof Double || v instanceof Float)) {
                top.put(name, n.longValue());
                return true;
            }
            return false;
        }
        if ("duration_ms".equals(name)) {
            if (v instanceof Number n && n.doubleValue() >= 0 && Double.isFinite(n.doubleValue())) {
                top.put(name, n);
                return true;
            }
            return false;
        }
        if ("attempt".equals(name)) {
            if (v instanceof Number n && n.longValue() >= 1 && n.longValue() <= 1000) {
                top.put(name, n.intValue());
                return true;
            }
            return false;
        }
        Pattern p = TOP.get(name);
        if (p == null || !(v instanceof CharSequence)) {
            return false;
        }
        String s = v.toString();
        if (!p.matcher(s).matches()) {
            return false;
        }
        top.put(name, s);
        return true;
    }

    /** An attribute: a snake_case name, masked by name, free text scrubbed; at most 32. */
    private static void attr(Map<String, Object> attrs, String key, Object value) {
        String name = attrName(key);
        if (attrs.size() >= MAX_ATTRS - 1 && !attrs.containsKey(name)) {
            attrs.merge("attrs_dropped", 1L, (a, b) -> (Long) a + (Long) b);
            return;
        }
        Object plain = value instanceof Number || value instanceof Boolean || value instanceof Map<?, ?>
                || value instanceof java.util.Collection<?> || value instanceof Phone ? value : String.valueOf(value);
        attrs.put(name, Pii.maskField(key, plain));
    }

    private static String attrName(String key) {
        String n = Pii.normalize(key).replaceAll("[^a-z0-9_]", "_");
        if (n.isEmpty() || !Character.isLetter(n.charAt(0))) {
            n = "f_" + n;
        }
        n = n.length() > 64 ? n.substring(0, 64) : n;
        return ATTR.matcher(n).matches() ? n : "f_attr";
    }

    private static Map<String, Object> error(ILoggingEvent e, Object code, Object kind, Object message,
            Map<String, Object> attrs) {
        Map<String, Object> error = new LinkedHashMap<>();
        if (code != null) {
            String c = code.toString();
            if (CODE.matcher(c).matches()) {
                error.put("code", c);
            } else {
                attrs.put("error_code", Pii.scrub(clip(c, 128)));
            }
        }
        IThrowableProxy t = e.getThrowableProxy();
        String k = kind instanceof CharSequence s && KIND.matcher(s).matches() ? s.toString()
                : t != null && KIND.matcher(t.getClassName()).matches() ? t.getClassName() : null;
        if (k != null) {
            error.put("kind", k);
        }
        String text = message != null ? message.toString() : t != null ? t.getMessage() : null;
        if (text != null && !text.isBlank()) {
            error.put("message", clip(Pii.scrub(text), MAX_ERROR_MESSAGE));
        }
        if (t != null && e.getLevel() != null && e.getLevel().isGreaterOrEqual(Level.ERROR)) {
            error.put("stack", stack(t));
        }
        return error;
    }

    /** Frames joined by newlines, causes as {@code caused by: <kind>: <message>}, 8192 characters at most. */
    static String stack(IThrowableProxy t) {
        StringBuilder b = new StringBuilder(1024);
        int depth = 0;
        for (IThrowableProxy p = t; p != null && depth < 10 && b.length() < MAX_STACK; p = p.getCause(), depth++) {
            if (depth > 0) {
                b.append("caused by: ").append(p.getClassName()).append(": ")
                        .append(Pii.scrub(String.valueOf(p.getMessage()))).append('\n');
            }
            StackTraceElementProxy[] frames = p.getStackTraceElementProxyArray();
            if (frames == null) {
                continue;
            }
            for (int i = 0; i < Math.min(frames.length, MAX_FRAMES); i++) {
                b.append("at ").append(frames[i].getStackTraceElement()).append('\n');
            }
        }
        if (!b.isEmpty() && b.charAt(b.length() - 1) == '\n') {
            b.setLength(b.length() - 1);
        }
        if (b.length() > MAX_STACK) {
            b.setLength(MAX_STACK - TRUNCATED.length());
            b.append(TRUNCATED);
        }
        return b.toString();
    }

    /** Where this service logged, when the caller data was taken (warn and above). */
    private Map<String, Object> code(ILoggingEvent e) {
        if (!e.hasCallerData()) {
            return null;
        }
        for (StackTraceElement f : e.getCallerData()) {
            String cls = f.getClassName();
            if (cls.startsWith(own) && !cls.startsWith(own + "obs.") && f.getFileName() != null) {
                int dot = cls.lastIndexOf('.');
                String dir = dot < 0 ? "" : cls.substring(0, dot).replace('.', '/') + "/";
                Map<String, Object> code = new LinkedHashMap<>();
                code.put("file", "src/main/java/" + dir + f.getFileName());
                if (f.getLineNumber() > 0) {
                    code.put("line", f.getLineNumber());
                }
                code.put("func", clip(cls.substring(dot + 1) + "." + f.getMethodName(), 256));
                return code;
            }
        }
        return null;
    }

    /** The line, shortened (never split) to {@value #MAX_LINE} bytes. */
    @SuppressWarnings("unchecked")
    private static String fit(Map<String, Object> line) {
        String out = write(line);
        if (out.length() * 3L <= MAX_LINE || out.getBytes(StandardCharsets.UTF_8).length <= MAX_LINE) {
            return out + "\n";
        }
        Object attrs = line.get("attrs");
        if (attrs instanceof Map<?, ?> m) {
            ((Map<String, Object>) m).replaceAll((k, v) -> v instanceof String s ? clip(s, 200) : v);
        }
        if (line.get("error") instanceof Map<?, ?> err && err.get("stack") instanceof String s) {
            ((Map<String, Object>) err).put("stack", clip(s, 2000));
        }
        out = write(line);
        if (out.getBytes(StandardCharsets.UTF_8).length <= MAX_LINE) {
            return out + "\n";
        }
        Map<String, Object> cut = new LinkedHashMap<>();
        cut.put("truncated", true);
        line.put("attrs", cut);
        if (line.get("error") instanceof Map<?, ?> err) {
            ((Map<String, Object>) err).remove("stack");
        }
        return write(line) + "\n";
    }

    private static String write(Map<String, Object> line) {
        StringBuilder out = new StringBuilder(512);
        Json.value(out, line);
        return out.toString();
    }

    static String clip(String s, int max) {
        if (s.length() <= max) {
            return s;
        }
        return s.substring(0, max - TRUNCATED.length()) + TRUNCATED;
    }
}
