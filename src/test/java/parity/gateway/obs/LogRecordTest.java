package parity.gateway.obs;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.KeyValuePair;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * log-record.v1 lines (parity-contracts CONVENTIONS "Logging"): the envelope, the lifted
 * correlation fields, masking, errors, the size cap and dedup. The records logged through slf4j
 * here also land in the captured test log that scripts/ci.sh checks with contract-check, so the
 * validator itself judges the masking of these values.
 */
class LogRecordTest {

    private static final Logger log = LoggerFactory.getLogger(LogRecordTest.class);
    private static final Logger library = LoggerFactory.getLogger("org.example.SomeLibrary");
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final String XSID = "0192f3c1-0000-7000-8000-000000000001";
    private static final JsonLogFormatter FORMATTER = new JsonLogFormatter(
            new JsonLogFormatter.Identity("parity-gateway", "0.1.0", "gw-1", "zone-1", "bin-a"));

    private static LoggingEvent event(Level level, String logger, String msg, Throwable t, Object... kv) {
        LoggerContext ctx = (LoggerContext) LoggerFactory.getILoggerFactory();
        LoggingEvent e = new LoggingEvent(LogRecordTest.class.getName(), ctx.getLogger(logger), level, msg, t, null);
        for (int i = 0; i < kv.length; i += 2) {
            e.addKeyValuePair(new KeyValuePair((String) kv[i], kv[i + 1]));
        }
        return e;
    }

    private static JsonNode format(LoggingEvent e) {
        String line = FORMATTER.format(e);
        assertThat(line).endsWith("\n").doesNotContain("\n\n");
        assertThat(line.strip()).doesNotContain("\n");
        return JSON.readTree(line);
    }

    @Test
    void theEnvelopeAndCorrelationFieldsAreTopLevelAndTheRestIsMaskedInAttrs() {
        JsonNode r = format(event(Level.INFO, "parity.gateway.agents.session.AgentSession", "offer sent to the desktop", null,
                "event", "offer.delivered", "tenant_id", "acme", "interaction_id", XSID, "agent_id", "agent-a1",
                "session_id", "s-1", "offer_id", "o-1", "callerNumber", "+447700900123", "contact_email", "ann@example.com",
                "dtmf_digits", "4111", "note", "call +447700900456 or bob@example.org", "duration_ms", 12L, "outcome", "ok"));
        assertThat(r.get("ts").asString()).matches("^\\d{4}-\\d\\d-\\d\\dT\\d\\d:\\d\\d:\\d\\d\\.\\d{3}Z$");
        assertThat(r.get("level").asString()).isEqualTo("info");
        assertThat(r.get("service").asString()).isEqualTo("parity-gateway");
        assertThat(r.get("seq").asLong()).isPositive();
        assertThat(r.get("incarnation").asString()).isNotBlank();
        assertThat(r.get("event").asString()).isEqualTo("offer.delivered");
        assertThat(r.get("xsid").asString()).isEqualTo(XSID);
        assertThat(r.has("interaction_id")).isFalse();
        assertThat(r.get("tenant_id").asString()).isEqualTo("acme");
        assertThat(r.get("zone").asString()).isEqualTo("zone-1");
        assertThat(r.get("bin").asString()).isEqualTo("bin-a");
        assertThat(r.get("outcome").asString()).isEqualTo("ok");
        assertThat(r.get("duration_ms").asLong()).isEqualTo(12);
        JsonNode attrs = r.get("attrs");
        assertThat(attrs.get("offer_id").asString()).isEqualTo("o-1");
        assertThat(attrs.get("caller_number").asString()).isEqualTo("***123");
        assertThat(attrs.get("contact_email").asString()).isEqualTo("[email]");
        assertThat(attrs.get("dtmf_digits").asString()).isEqualTo("[redacted]");
        assertThat(attrs.get("note").asString()).isEqualTo("call ***456 or [email]");
        assertThat(attrs.has("tenant_id")).isFalse();
    }

    @Test
    void aLibrarysTextIsScrubbedAndItsRecordIsExternal() {
        JsonNode r = format(event(Level.WARN, "org.example.SomeLibrary",
                "dialing sip:+447700900123@carrier.example for ann@example.com via sip:agent-1@acme.example", null));
        assertThat(r.get("event").asString()).isEqualTo("log.external");
        assertThat(r.get("level").asString()).isEqualTo("warn");
        // ***123@carrier.example would still read as an address to the validator: the whole part goes.
        assertThat(r.get("msg").asString()).isEqualTo("dialing sip:***[email] for [email] via sip:agent-1@acme.example");
        assertThat(r.get("attrs").get("logger").asString()).isEqualTo("org.example.SomeLibrary");
    }

    @Test
    void aRecordOfThisServiceWithoutAnEventIsUnclassifiedAndALibrarysUnderOurNameIsExternal() {
        assertThat(format(event(Level.INFO, "parity.gateway.Anything", "x", null, "k", "v")).get("event").asString())
                .isEqualTo("log.unclassified");
        // Spring's startup lines and its filters' debug lines use our class names, with no key-value pairs.
        JsonNode spring = format(event(Level.INFO, "parity.gateway.ParityGatewayApplication", "Started in 2.1 seconds", null));
        assertThat(spring.get("event").asString()).isEqualTo("log.external");
        assertThat(spring.get("attrs").get("logger").asString()).isEqualTo("parity.gateway.ParityGatewayApplication");
    }

    @Test
    void anInteractionIdThatIsntAUuid7StaysInteractionIdAndBadValuesGoToAttrs() {
        JsonNode r = format(event(Level.INFO, "parity.gateway.X", "x", null, "event", "readiness.passed",
                "interaction_id", "6f1c2a9e-0000-4000-8000-000000000001", "tenant_id", "Not A Tenant", "cause", "IOException"));
        assertThat(r.has("xsid")).isFalse();
        assertThat(r.get("interaction_id").asString()).isEqualTo("6f1c2a9e-0000-4000-8000-000000000001");
        assertThat(r.has("tenant_id")).isFalse();
        assertThat(r.get("attrs").get("tenant_id").asString()).isEqualTo("Not A Tenant");
        assertThat(r.get("attrs").get("cause").asString()).isEqualTo("IOException");
    }

    @Test
    void errorsHaveKindAndMessageAndAStackOnlyAtError() {
        Exception e = new IllegalStateException("callee +447700900123 unreachable",
                new java.io.IOException("reset by peer"));
        JsonNode warn = format(event(Level.WARN, "parity.gateway.X", "x", e, "event", "session.task_failed", "error_code", "unavailable"));
        assertThat(warn.get("error").get("code").asString()).isEqualTo("unavailable");
        assertThat(warn.get("error").get("kind").asString()).isEqualTo("java.lang.IllegalStateException");
        assertThat(warn.get("error").get("message").asString()).isEqualTo("callee ***123 unreachable");
        assertThat(warn.get("error").has("stack")).isFalse();
        JsonNode error = format(event(Level.ERROR, "parity.gateway.X", "x", e, "event", "session.task_failed"));
        String stack = error.get("error").get("stack").asString();
        assertThat(stack).startsWith("at ").contains("\ncaused by: java.io.IOException: reset by peer\n").hasSizeLessThanOrEqualTo(8192);
    }

    @Test
    void aLineOverTheCapIsShortenedNotSplit() {
        String big = "x".repeat(3000);
        Object[] kv = new Object[2 + 20 * 2];
        kv[0] = "event";
        kv[1] = "readiness.passed";
        for (int i = 0; i < 20; i++) {
            kv[2 + 2 * i] = "field_" + i;
            kv[3 + 2 * i] = big;
        }
        String line = FORMATTER.format(event(Level.INFO, "parity.gateway.X", "x", null, kv));
        assertThat(line.getBytes(StandardCharsets.UTF_8).length).isLessThanOrEqualTo(16_001);
        assertThat(JSON.readTree(line).get("attrs").get("field_0").asString()).endsWith("...[truncated]");
    }

    @Test
    void dedupHoldsRepeatsAndCountsThem() throws Exception {
        LoggerContext ctx = (LoggerContext) LoggerFactory.getILoggerFactory();
        ListAppender<ILoggingEvent> out = new ListAppender<>();
        out.setContext(ctx);
        out.start();
        CountingAsyncAppender a = new CountingAsyncAppender();
        a.setContext(ctx);
        a.setCatalogue("logging/events.parity-gateway.v1.json");
        a.addAppender(out);
        a.start();
        for (int i = 0; i < 5; i++) {
            a.doAppend(event(Level.WARN, "parity.gateway.X", "a desktop frame was refused", null,
                    "event", "frame.refused", "session_id", "s-1", "error_code", "rate_limited"));
        }
        a.doAppend(event(Level.WARN, "parity.gateway.X", "a desktop frame was refused", null,
                "event", "frame.refused", "session_id", "s-2", "error_code", "rate_limited"));
        a.stop();
        List<JsonNode> lines = out.list.stream().map(e -> JSON.readTree(FORMATTER.format(e))).toList();
        assertThat(lines).hasSize(3);
        assertThat(lines.get(0).has("repeat_count")).isFalse();
        JsonNode summary = lines.stream().filter(l -> l.has("repeat_count")).findFirst().orElseThrow();
        assertThat(summary.get("repeat_count").asLong()).isEqualTo(4);
        assertThat(summary.get("session_id").asString()).isEqualTo("s-1");
        assertThat(summary.has("repeat_since")).isTrue();
        assertThat(lines.stream().map(l -> l.get("seq").asLong()).distinct()).hasSize(3);
    }

    /** Into the captured log, for contract-check: values the masking rules must catch. */
    @Test
    void trickyValuesReachTheCapturedLogMasked() {
        log.atInfo().addKeyValue("event", "offer.delivered").addKeyValue("tenant_id", "acme").addKeyValue("xsid", XSID)
                .addKeyValue("agent_id", "agent-a1").addKeyValue("session_id", "s-1").addKeyValue("offer_id", "o-1")
                .addKeyValue("source", "watch").addKeyValue("from", "+447700900123").addKeyValue("email", "ann@example.com")
                .addKeyValue("pin", "1234").addKeyValue("sip_call_id", "a84b@+447700900999")
                .addKeyValue("headers", java.util.Map.of("To", "<sip:+447700900456@x>", "X-Note", "mail bob@example.org"))
                .log("offer sent to the desktop");
        library.atWarn().log("connecting to tel:+447700900123 for carol@example.net");
        log.atError().addKeyValue("event", "session.task_failed").setCause(new IllegalStateException("bad +447700900777"))
                .log("a session task failed");
        assertThat(CountingAsyncAppender.dropped()).isGreaterThanOrEqualTo(0);
    }
}
