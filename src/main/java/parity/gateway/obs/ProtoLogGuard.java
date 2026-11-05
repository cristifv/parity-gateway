package parity.gateway.obs;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.util.JsonFormat;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;

/**
 * The net under the rule "never log a protobuf message" (TC ADR 0001 §4.7, R1-5): a message's
 * {@code toString()} prints every field, which masking by name can't see. The log formatter hands
 * any protobuf argument or key value here instead, gets back its fields as a masked map, and the
 * count of such lines is a metric ({@code parity_gateway_log_proto_args}): it should stay 0.
 */
public final class ProtoLogGuard {

    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final JsonFormat.Printer PRINTER = JsonFormat.printer().preservingProtoFieldNames();
    private static final AtomicLong CAUGHT = new AtomicLong();

    private ProtoLogGuard() {}

    /** A loggable value: a protobuf message becomes its masked fields; anything else is unchanged. */
    public static Object safe(Object value) {
        if (!(value instanceof MessageOrBuilder m)) {
            return value;
        }
        CAUGHT.incrementAndGet();
        try {
            Map<String, Object> fields = JSON.readValue(PRINTER.print(m), new TypeReference<Map<String, Object>>() {});
            return Pii.maskNested(maskDeep(fields));
        } catch (InvalidProtocolBufferException | RuntimeException e) {
            return "[" + m.getDescriptorForType().getName() + "]";
        }
    }

    /** Lines that carried a protobuf message. */
    public static long caught() {
        return CAUGHT.get();
    }

    /** {@link Pii#maskNested} masks one level of keys; nested messages are masked here, depth first. */
    private static Object maskDeep(Object value) {
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> out = new java.util.LinkedHashMap<>();
            map.forEach((k, v) -> out.put(String.valueOf(k),
                    v instanceof Map<?, ?> || v instanceof java.util.List<?> ? Pii.maskNested(maskDeep(v)) : v));
            return out;
        }
        if (value instanceof java.util.List<?> items) {
            return items.stream().map(ProtoLogGuard::maskDeep).toList();
        }
        return value;
    }
}
