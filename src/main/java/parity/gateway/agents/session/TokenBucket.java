package parity.gateway.agents.session;

import java.util.concurrent.locks.ReentrantLock;

/** A local token bucket (rate per second, burst). */
public final class TokenBucket {

    private final ReentrantLock lock = new ReentrantLock();
    private final double perNano;
    private final double burst;
    private double tokens;
    private long last = System.nanoTime();

    public TokenBucket(double perSecond, double burst) {
        this.perNano = perSecond / 1e9;
        this.burst = burst;
        this.tokens = burst;
    }

    public boolean tryTake(long nowNanos) {
        lock.lock();
        try {
            tokens = Math.min(burst, tokens + (nowNanos - last) * perNano);
            last = nowNanos;
            if (tokens >= 1) {
                tokens -= 1;
                return true;
            }
            return false;
        } finally {
            lock.unlock();
        }
    }
}
