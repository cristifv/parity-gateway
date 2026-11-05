package parity.gateway.agents;

import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.MetadataUtils;
import java.time.Duration;
import java.util.HexFormat;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import parity.gateway.config.GatewayProperties.Endpoint;

/**
 * What the RE and IE clients share: the channel, the metadata ({@code tenant-id},
 * {@code traceparent}; {@code x-parity-test-peer} only when configured, GW-012), the
 * {@code parity-error} trailer, and retries with full jitter reusing the same request (so the
 * same {@code command_id}).
 */
public final class GrpcCalls {

    public static final Metadata.Key<String> PARITY_ERROR = Metadata.Key.of("parity-error", Metadata.ASCII_STRING_MARSHALLER);
    public static final Metadata.Key<String> TENANT = Metadata.Key.of("tenant-id", Metadata.ASCII_STRING_MARSHALLER);
    public static final Metadata.Key<String> TRACEPARENT = Metadata.Key.of("traceparent", Metadata.ASCII_STRING_MARSHALLER);
    public static final Metadata.Key<String> TEST_PEER = Metadata.Key.of("x-parity-test-peer", Metadata.ASCII_STRING_MARSHALLER);

    /** A refused or failed call: the gRPC status and the {@code parity-error} code. */
    public static final class Failed extends Exception {
        private static final long serialVersionUID = 1L;
        private final Status.Code status;
        private String code;

        public Failed(Status.Code status, String code) {
            super("gRPC call failed", null, false, false);
            this.status = status;
            this.code = code;
        }

        public Status.Code status() {
            return status;
        }

        /** The parity-error trailer, or {@code unavailable}/{@code internal} for transport faults. */
        public String code() {
            return code;
        }

        public boolean retryable() {
            return status == Status.Code.UNAVAILABLE || status == Status.Code.INTERNAL
                    || status == Status.Code.DEADLINE_EXCEEDED;
        }
    }

    private GrpcCalls() {}

    public static ManagedChannel channel(Endpoint endpoint) {
        return NettyChannelBuilder.forAddress(endpoint.host(), endpoint.port()).usePlaintext()
                .keepAliveTime(30, TimeUnit.SECONDS).maxInboundMessageSize(1 << 20).build();
    }

    /** The channel with this call's metadata attached. */
    public static Channel with(Channel channel, String tenantId, String peer, boolean testPeer) {
        Metadata md = new Metadata();
        if (tenantId != null) {
            md.put(TENANT, tenantId);
        }
        md.put(TRACEPARENT, traceparent());
        if (testPeer) {
            md.put(TEST_PEER, peer);
        }
        return ClientInterceptors.intercept(channel, MetadataUtils.newAttachHeadersInterceptor(md));
    }

    public static Failed failed(StatusRuntimeException e) {
        Metadata trailers = e.getTrailers();
        String code = trailers == null ? null : trailers.get(PARITY_ERROR);
        Status.Code status = e.getStatus().getCode();
        if (code == null) {
            code = switch (status) {
                case UNAVAILABLE, DEADLINE_EXCEEDED -> "unavailable";
                default -> "internal";
            };
        }
        Failed f = new Failed(status, "internal");
        f.code = code;
        return f;
    }

    /**
     * Calls {@code attempt} with a per-try deadline, up to {@code tries} times for retryable
     * failures, backoff from {@code firstBackoff} with full jitter, never past {@code budget}.
     */
    public static <T> T call(Function<Duration, T> attempt, int tries, Duration perTry, Duration firstBackoff,
            Duration budget) throws Failed {
        long deadline = System.nanoTime() + budget.toNanos();
        Duration backoff = firstBackoff;
        Failed last = new Failed(Status.Code.UNAVAILABLE, "unavailable");
        for (int i = 0; i < tries; i++) {
            long left = deadline - System.nanoTime();
            if (left <= 0) {
                break;
            }
            try {
                return attempt.apply(Duration.ofNanos(Math.min(left, perTry.toNanos())));
            } catch (StatusRuntimeException e) {
                last = failed(e);
                if (!last.retryable() || i == tries - 1) {
                    throw last;
                }
                Duration pause = "owner_moving".equals(last.code()) ? Duration.ofSeconds(1)
                        : Duration.ofNanos(ThreadLocalRandom.current().nextLong(backoff.toNanos() + 1));
                backoff = backoff.multipliedBy(2);
                if (pause.toNanos() >= deadline - System.nanoTime()) {
                    throw last;
                }
                try {
                    Thread.sleep(pause);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw last;
                }
            }
        }
        throw last;
    }

    private static String traceparent() {
        ThreadLocalRandom r = ThreadLocalRandom.current();
        HexFormat hex = HexFormat.of();
        byte[] trace = new byte[16];
        byte[] span = new byte[8];
        r.nextBytes(trace);
        r.nextBytes(span);
        return "00-" + hex.formatHex(trace) + "-" + hex.formatHex(span) + "-01";
    }
}
