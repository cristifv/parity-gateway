package parity.gateway.agents;

import java.security.SecureRandom;
import java.util.UUID;

/** RFC 9562 UUIDv7 ids for the tests' interactions: a log record's xsid must be one (log-record.v1). */
public final class Uuid7 {

    private static final SecureRandom RANDOM = new SecureRandom();

    private Uuid7() {}

    public static String next() {
        long ms = System.currentTimeMillis();
        long hi = (ms << 16) | 0x7000L | (RANDOM.nextInt() & 0x0fffL);
        long lo = (RANDOM.nextLong() & 0x3fffffffffffffffL) | 0x8000000000000000L;
        return new UUID(hi, lo).toString();
    }
}
