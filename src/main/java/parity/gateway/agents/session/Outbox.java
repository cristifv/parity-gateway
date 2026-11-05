package parity.gateway.agents.session;

import jakarta.websocket.SendResult;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntConsumer;
import tools.jackson.databind.node.ObjectNode;

/**
 * A session's frames to the desktop, sent one at a time with the container's asynchronous send:
 * no thread waits for a slow desktop. Bounded: past {@code limit} queued frames the socket closes
 * (1008), since a desktop that far behind shows a wrong state anyway.
 */
final class Outbox {

    private final jakarta.websocket.Session socket;
    private final ConcurrentLinkedQueue<String> queue = new ConcurrentLinkedQueue<>();
    private final AtomicInteger size = new AtomicInteger();
    private final AtomicBoolean sending = new AtomicBoolean();
    private final int limit;
    private final IntConsumer onFailure;
    /** Written only by the session's executor. */
    private long seq;

    Outbox(jakarta.websocket.Session socket, int limit, IntConsumer onFailure) {
        this.socket = socket;
        this.limit = limit;
        this.onFailure = onFailure;
    }

    /** Called on the session's executor: numbers the frame and queues it. */
    void send(ObjectNode frame) {
        frame.put("seq", ++seq);
        if (size.incrementAndGet() > limit) {
            onFailure.accept(1008);
            return;
        }
        queue.add(frame.toString());
        pump();
    }

    private void pump() {
        if (!sending.compareAndSet(false, true)) {
            return;
        }
        String next = queue.poll();
        if (next == null) {
            sending.set(false);
            if (!queue.isEmpty()) {
                pump();
            }
            return;
        }
        size.decrementAndGet();
        try {
            socket.getAsyncRemote().sendText(next, (SendResult result) -> {
                sending.set(false);
                if (!result.isOK()) {
                    onFailure.accept(1011);
                    return;
                }
                Thread.startVirtualThread(this::pump);
            });
        } catch (RuntimeException e) {
            sending.set(false);
            onFailure.accept(1011);
        }
    }
}
