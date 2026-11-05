package parity.gateway.agents.session;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs one session's tasks in order, one at a time, on virtual threads: the single writer of the
 * session's view. Tasks may block (a gRPC call); nothing holds a monitor while they do.
 */
final class SerialExecutor {

    private static final Logger log = LoggerFactory.getLogger(SerialExecutor.class);

    private final ConcurrentLinkedQueue<Runnable> tasks = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean running = new AtomicBoolean();
    private final String name;

    SerialExecutor(String name) {
        this.name = name;
    }

    void execute(Runnable task) {
        tasks.add(task);
        if (running.compareAndSet(false, true)) {
            Thread.ofVirtual().name(name).start(this::drain);
        }
    }

    private void drain() {
        while (true) {
            Runnable task = tasks.poll();
            if (task == null) {
                running.set(false);
                if (tasks.isEmpty() || !running.compareAndSet(false, true)) {
                    return;
                }
                continue;
            }
            try {
                task.run();
            } catch (RuntimeException e) {
                log.atError().addKeyValue("event", "session.task_failed").setCause(e).log("a session task failed");
            }
        }
    }
}
