package parity.gateway.obs;

import java.util.Collection;
import java.util.Map;

/**
 * A small JSON writer for log lines: no library on the logging path, so a log line can be
 * written while anything else (Jackson included) is still starting or already stopped.
 */
final class Json {

    private Json() {}

    static void value(StringBuilder out, Object v) {
        if (v == null) {
            out.append("null");
        } else if (v instanceof Boolean || v instanceof Integer || v instanceof Long
                || v instanceof Short || v instanceof Byte) {
            out.append(v);
        } else if (v instanceof Number n) {
            double d = n.doubleValue();
            if (Double.isFinite(d)) {
                out.append(n);
            } else {
                string(out, n.toString());
            }
        } else if (v instanceof Map<?, ?> map) {
            out.append('{');
            boolean first = true;
            for (Map.Entry<?, ?> e : map.entrySet()) {
                if (!first) {
                    out.append(',');
                }
                first = false;
                string(out, String.valueOf(e.getKey()));
                out.append(':');
                value(out, e.getValue());
            }
            out.append('}');
        } else if (v instanceof Collection<?> items) {
            out.append('[');
            boolean first = true;
            for (Object item : items) {
                if (!first) {
                    out.append(',');
                }
                first = false;
                value(out, item);
            }
            out.append(']');
        } else {
            string(out, String.valueOf(v));
        }
    }

    static void string(StringBuilder out, String s) {
        out.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t");
                default -> {
                    if (c < 0x20 || c == 0x2028 || c == 0x2029) {
                        out.append(String.format("\\u%04x", (int) c));
                    } else {
                        out.append(c);
                    }
                }
            }
        }
        out.append('"');
    }
}
