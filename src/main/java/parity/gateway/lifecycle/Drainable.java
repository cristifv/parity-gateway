package parity.gateway.lifecycle;

import java.time.Duration;

/** Work the drain waits for: sessions and streams from steps 3 to 5. */
public interface Drainable {

    /** Stops taking new work and waits for the work in progress; false if time ran out. */
    boolean drain(Duration timeout) throws InterruptedException;
}
