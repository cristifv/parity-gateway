package parity.gateway.obs;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Personal data in logs (ARCHITECTURE.md §5.2): the same rules as interaction-engine's
 * {@code ie-obs} ({@code crates/ie-obs/src/pii.rs}), checked by both against parity-contracts'
 * {@code masking/cases.v1.json}.
 * <ul>
 *   <li>{@link #normalize}: camelCase split ({@code callerNumber} → {@code caller_number},
 *       {@code DTMFDigit} → {@code dtmf_digit}), ASCII lowercase, {@code -} → {@code _}, a
 *       leading {@code variable_} (freeswitch channel variables) stripped;</li>
 *   <li>{@link #classify}: phone (masked to its last 3 digits), secret ({@code [redacted]}) or
 *       plain, by the normalized field name, whatever the value's type;</li>
 *   <li>{@link #maskField}: the policy applied to a value, at every depth.</li>
 * </ul>
 *
 * <p><b>Exception messages carry no data:</b> no phone number, digit, SDP, key or other personal
 * value, because they reach logs unmasked (the formatter masks fields, not messages). They may
 * name configuration (a property, a path, a {@code host:port}).
 */
public final class Pii {

    /** What the formatter does with a field. */
    public enum FieldClass { PHONE, SECRET, EMAIL, PLAIN }

    public static final String REDACTED = "[redacted]";
    /** What an email address becomes (masking/billing.v1.json, 0.10.2): nothing of it, not even a hash. */
    public static final String EMAIL = "[email]";

    private static final List<String> PHONE_NAMES = List.of(
            "from", "to", "number", "phone", "caller", "callee", "caller_id", "callback_number",
            "destination", "ani", "dnis", "msisdn", "e164", "contact", "uri", "pai",
            "p_asserted_identity", "rpid", "remote_party_id", "diversion", "history_info", "user",
            // freeswitch ESL names that carry numbers (ie-obs F-C).
            "channel_name", "caller_channel_name", "caller_username", "rdnis", "dialed_extension",
            "originate_string", "dial_string", "sip_from_display", "caller_caller_id_name",
            "sip_destination_url", "sip_call_id");
    private static final List<String> SECRET_NAMES = List.of(
            "digits", "dtmf", "pin", "password", "secret", "token", "authorization", "api_key", "dek",
            "wrapped_dek", "sdp", "sip_body", "passwd", "credentials",
            // masking/billing.v1.json (log-record.v1 checks every service against both files)
            "client_secret", "access_token", "refresh_token", "id_token", "secret_key", "signature",
            "cf_turnstile_response", "checkout_url", "portal_url", "verification_url");
    /** masking/billing.v1.json's email class, and any name ending {@code _email}. */
    private static final List<String> EMAIL_NAMES = List.of("email", "admin_email", "billing_email", "contact_email");
    /** Any name with one of these {@code _}-separated tokens is a secret (DTMF digits: PCI). */
    private static final Set<String> SECRET_TOKENS = Set.of("dtmf", "digit", "digits");
    /** Names a rule would catch that are known not to hold personal data. */
    private static final Set<String> EXEMPT = Set.of("reply_to", "num_digits");

    private Pii() {}

    /**
     * The name the policy sees (parity-contracts' {@code masking/cases.v1.json}, "normalize"):
     * <ol>
     *   <li>an {@code _} goes before an ASCII uppercase letter that follows an ASCII lowercase
     *       letter or an ASCII digit, or that follows an ASCII uppercase letter and is followed
     *       by an ASCII lowercase letter ({@code callerNumber} → {@code caller_Number},
     *       {@code DTMFDigit} → {@code DTMF_Digit}, {@code Caller-ID} unchanged);</li>
     *   <li>ASCII uppercase → lowercase; {@code -} → {@code _};</li>
     *   <li>a leading {@code variable_} is stripped.</li>
     * </ol>
     */
    public static String normalize(String name) {
        StringBuilder b = new StringBuilder(name.length() + 4);
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            if (upper(c) && i > 0) {
                char prev = name.charAt(i - 1);
                boolean next = i + 1 < name.length() && lower(name.charAt(i + 1));
                if (lower(prev) || asciiDigit(prev) || (upper(prev) && next)) {
                    b.append('_');
                }
            }
            if (upper(c)) {
                b.append((char) (c + ('a' - 'A')));
            } else if (c == '-') {
                b.append('_');
            } else {
                b.append(c);
            }
        }
        String lower = b.toString();
        return lower.startsWith("variable_") ? lower.substring("variable_".length()) : lower;
    }

    /** {@code name} equals {@code listed}, or ends with {@code _} + {@code listed}. */
    private static boolean matches(String name, String listed) {
        return name.equals(listed)
                || (name.length() > listed.length()
                        && name.endsWith(listed)
                        && name.charAt(name.length() - listed.length() - 1) == '_');
    }

    /** The policy for a field name (normalized first). */
    public static FieldClass classify(String name) {
        String n = normalize(name);
        if (EXEMPT.contains(n)) {
            return FieldClass.PLAIN;
        }
        if (EMAIL_NAMES.stream().anyMatch(e -> matches(n, e))) {
            return FieldClass.EMAIL;
        }
        for (String token : n.split("_", -1)) {
            if (SECRET_TOKENS.contains(token)) {
                return FieldClass.SECRET;
            }
        }
        if (SECRET_NAMES.stream().anyMatch(s -> matches(n, s))
                || n.endsWith("_secret") || n.endsWith("_token") || n.endsWith("_password")) {
            return FieldClass.SECRET;
        }
        if (PHONE_NAMES.stream().anyMatch(p -> matches(n, p))
                || n.endsWith("_number") || n.endsWith("_phone") || n.endsWith("_user")) {
            return FieldClass.PHONE;
        }
        return FieldClass.PLAIN;
    }

    private static boolean asciiDigit(char c) {
        return c >= '0' && c <= '9';
    }

    private static boolean upper(char c) {
        return c >= 'A' && c <= 'Z';
    }

    private static boolean lower(char c) {
        return c >= 'a' && c <= 'z';
    }

    /**
     * Keeps the last 3 ASCII digits behind {@code ***} when there are at least 4; otherwise
     * {@code ***}. A value already masked ({@code ***123}, {@code Phone(***123)}) keeps its
     * last 3 digits rather than being masked again.
     */
    public static String maskPhone(String raw) {
        StringBuilder digits = new StringBuilder();
        for (int i = 0; i < raw.length(); i++) {
            if (asciiDigit(raw.charAt(i))) {
                digits.append(raw.charAt(i));
            }
        }
        int at = raw.indexOf("***");
        if (at >= 0) {
            StringBuilder tail = new StringBuilder();
            for (int i = at + 3; i < raw.length() && asciiDigit(raw.charAt(i)); i++) {
                tail.append(raw.charAt(i));
            }
            if (tail.length() > 0 && tail.length() <= 3 && tail.length() == digits.length()) {
                return "***" + tail;
            }
        }
        if (digits.length() >= 4) {
            return "***" + digits.substring(digits.length() - 3);
        }
        return "***";
    }

    /** Applies the policy to a value logged under {@code name}. */
    public static Object maskField(String name, Object value) {
        return switch (classify(name)) {
            case PLAIN -> maskNested(value);
            case EMAIL -> value == null ? null : EMAIL;
            case SECRET -> REDACTED;
            case PHONE -> maskAll(value);
        };
    }

    /** Recurses into maps and collections, applying the policy to every key. */
    public static Object maskNested(Object value) {
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> out = new LinkedHashMap<>();
            map.forEach((k, v) -> out.put(String.valueOf(k), maskField(String.valueOf(k), v)));
            return out;
        }
        if (value instanceof Collection<?> items) {
            List<Object> out = new ArrayList<>(items.size());
            items.forEach(item -> out.add(maskNested(item)));
            return out;
        }
        if (value instanceof Phone phone) {
            return phone.toString();
        }
        if (value instanceof String text) {
            return scrub(text);
        }
        return value;
    }

    private static final Pattern SIP_USER_DIGITS = Pattern.compile("(?i)(sips?:|tel:)\\+?([0-9]{7,})");
    private static final Pattern E164_IN_TEXT = Pattern.compile("\\+([1-9][0-9]{6,})");
    private static final Pattern EMAIL_IN_TEXT =
            Pattern.compile("(?i)(sips?:)?[a-z0-9._%+-]+@[a-z0-9.-]+\\.[a-z]{2,}");

    /**
     * Free text (a message, an exception's text, a string under a plain name) with the personal
     * data log-record.v1's validator scans for taken out: an E.164 number and a {@code sip:}/
     * {@code tel:} user part of 7 or more digits keep their last 3 digits ({@code ***123}); an
     * email address becomes {@link #EMAIL} (a SIP URI's {@code user@host} is left alone, as the
     * validator does). The net under the rule "never interpolate values into a message".
     */
    public static String scrub(String text) {
        if (text == null || text.isEmpty()) {
            return text;
        }
        String out = text;
        if (out.indexOf(':') >= 0) {
            out = SIP_USER_DIGITS.matcher(out).replaceAll(m -> Matcher.quoteReplacement(m.group(1) + maskPhone(m.group(2))));
        }
        if (out.indexOf('+') >= 0) {
            out = E164_IN_TEXT.matcher(out).replaceAll(m -> Matcher.quoteReplacement(maskPhone(m.group(1))));
        }
        if (out.indexOf('@') >= 0) {
            out = EMAIL_IN_TEXT.matcher(out).replaceAll(m -> Matcher.quoteReplacement(m.group(1) != null ? m.group() : EMAIL));
        }
        return out;
    }

    /** Everything under a phone key is masked, scalars and nested values alike. */
    public static Object maskAll(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> out = new LinkedHashMap<>();
            map.forEach((k, v) -> out.put(String.valueOf(k), maskAll(v)));
            return out;
        }
        if (value instanceof Collection<?> items) {
            List<Object> out = new ArrayList<>(items.size());
            items.forEach(item -> out.add(maskAll(item)));
            return out;
        }
        if (value instanceof Boolean) {
            return value;
        }
        return maskPhone(String.valueOf(value));
    }
}
