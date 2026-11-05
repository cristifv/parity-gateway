package parity.gateway.obs;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * An E.164 phone number that can only be printed masked: {@link #toString()} gives
 * {@code ***123}, so a number logged by mistake, in a message or a field, never shows. The raw
 * value comes only from {@link #expose()}, for the places that must send it (a SIP request).
 * {@code ie-obs}'s {@code Phone}.
 */
public final class Phone {

    private static final Pattern E164 = Pattern.compile("^\\+[1-9][0-9]{6,14}$");

    private final String value;

    private Phone(String value) {
        this.value = value;
    }

    /** {@code ^\+[1-9][0-9]{6,14}$}, or {@link IllegalArgumentException} (whose message holds no digit). */
    public static Phone parseE164(String raw) {
        if (raw == null || !E164.matcher(raw).matches()) {
            throw new IllegalArgumentException("not an E.164 number");
        }
        return new Phone(raw);
    }

    /** The raw number. Never log it. */
    public String expose() {
        return value;
    }

    @Override
    public String toString() {
        return Pii.maskPhone(value);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Phone p && p.value.equals(value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }
}
