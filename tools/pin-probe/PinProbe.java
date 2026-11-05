/**
 * scripts/check-pins.sh's own test (TC ADR 0002 R1-2): a virtual thread that sleeps inside
 * synchronized pins its carrier on Java 21, and -Djdk.tracePinnedThreads=short prints the trace
 * the check must find. Run by scripts/ci.sh as a single-file program; never part of TC.
 */
public final class PinProbe {
    private static final Object LOCK = new Object();

    public static void main(String[] args) throws Exception {
        Thread t = Thread.ofVirtual().start(() -> {
            synchronized (LOCK) {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        t.join();
    }
}
