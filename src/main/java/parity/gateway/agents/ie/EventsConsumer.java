package parity.gateway.agents.ie;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import parity.gateway.agents.AgentsMetrics;
import parity.gateway.agents.session.SessionRegistry;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Reads every partition of the bin's {@code interaction.events} from the latest offset (one
 * group per replica, GW-001): headers first; an interaction no session here shows is skipped
 * without parsing its body; an event whose header tenant isn't its body's is dropped and counted.
 * The event only says "look again": the session asks IE for the call (state is authoritative).
 */
public final class EventsConsumer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(EventsConsumer.class);
    private static final JsonMapper JSON = JsonMapper.builder().build();

    private final SessionRegistry sessions;
    private final AgentsMetrics metrics;
    private final Properties props = new Properties();
    private final String topic;
    private volatile boolean closed;
    private Thread thread;

    public EventsConsumer(String bootstrap, String topic, String replicaId, SessionRegistry sessions, AgentsMetrics metrics) {
        this.topic = topic;
        this.sessions = sessions;
        this.metrics = metrics;
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "parity-gateway.agents." + replicaId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    }

    public void start() {
        thread = Thread.ofPlatform().name("interaction-events").daemon(true).start(this::run);
    }

    private void run() {
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
            List<TopicPartition> parts = consumer.partitionsFor(topic).stream()
                    .map(p -> new TopicPartition(p.topic(), p.partition())).toList();
            consumer.assign(parts);
            consumer.seekToEnd(parts);
            while (!closed) {
                consumer.poll(Duration.ofMillis(500)).forEach(this::handle);
            }
        } catch (RuntimeException e) {
            if (!closed) {
                log.atError().addKeyValue("event", "interaction_events.stopped").addKeyValue("topic", topic).setCause(e)
                        .log("interaction.events consumer stopped");
            }
        }
    }

    /** One record: route it, or say why not. */
    public String handle(ConsumerRecord<byte[], byte[]> r) {
        String tenant = header(r, "tenant_id");
        String interaction = header(r, "interaction_id");
        if (tenant == null || interaction == null) {
            metrics.count("event_dropped", "missing_header");
            dropped("missing_header", r);
            return "missing_header";
        }
        if (!sessions.watches(interaction)) {
            return "not_watched";
        }
        JsonNode body;
        try {
            body = JSON.readTree(r.value());
        } catch (RuntimeException e) {
            metrics.count("event_dropped", "bad_body");
            dropped("bad_body", r);
            return "bad_body";
        }
        if (body == null || !tenant.equals(body.path("tenant_id").asString(""))
                || !interaction.equals(body.path("interaction_id").asString(""))) {
            metrics.count("event_dropped", "header_body_mismatch");
            dropped("header_body_mismatch", r);
            return "header_body_mismatch";
        }
        long ownerEpoch = body.path("owner_epoch").asLong(0);
        long seq = body.path("seq").asLong(0);
        log.atDebug().addKeyValue("event", "interaction_event.routed").addKeyValue("tenant_id", tenant)
                .addKeyValue("xsid", interaction).addKeyValue("event_id", body.path("event_id").asString(null))
                .addKeyValue("traceparent", header(r, "traceparent"))
                .addKeyValue("owner_epoch", ownerEpoch).addKeyValue("event_seq", seq)
                .addKeyValue("event_type", body.path("type").asString(null))
                .log("an interaction event went to the desktops showing the call");
        sessions.onEvent(tenant, interaction, ownerEpoch, seq);
        return "routed";
    }

    /** A record that can't be routed (dedup: one per reason and window). */
    private void dropped(String reason, ConsumerRecord<byte[], byte[]> r) {
        log.atWarn().addKeyValue("event", "interaction_event.dropped").addKeyValue("reason", reason)
                .addKeyValue("partition", r.partition()).addKeyValue("offset", r.offset())
                .log("an interaction event was dropped");
    }

    private static String header(ConsumerRecord<byte[], byte[]> r, String name) {
        Header h = r.headers().lastHeader(name);
        return h == null || h.value() == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }

    @Override
    public void close() {
        closed = true;
        if (thread != null) {
            thread.interrupt();
        }
    }
}
