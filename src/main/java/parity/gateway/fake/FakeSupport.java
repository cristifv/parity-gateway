package parity.gateway.fake;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.regex.Pattern;
import parity.gateway.agents.GrpcCalls;

/** What the fake RE and fake IE share: the caller's metadata and errors with {@code parity-error}. */
final class FakeSupport {

    static final Context.Key<String> TENANT = Context.key("tenant-id");
    static final Context.Key<String> PEER = Context.key("peer");
    static final Pattern UUID = Pattern.compile("^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$");
    static final Pattern COMMAND_ID = Pattern.compile("^[A-Za-z0-9._:-]{1,200}$");

    private FakeSupport() {}

    /** Captures tenant-id and the test peer; a caller without a gateway identity is refused (GW-012). */
    static ServerInterceptor callers() {
        return new ServerInterceptor() {
            @Override
            public <Q, R> ServerCall.Listener<Q> interceptCall(ServerCall<Q, R> call, Metadata headers,
                    ServerCallHandler<Q, R> next) {
                String peer = headers.get(GrpcCalls.TEST_PEER);
                if (peer == null || !(peer.startsWith("parity-gateway.") || peer.startsWith("parity-interaction-engine."))) {
                    Metadata t = new Metadata();
                    t.put(GrpcCalls.PARITY_ERROR, "identity_mismatch");
                    call.close(Status.PERMISSION_DENIED, t);
                    return new ServerCall.Listener<>() {};
                }
                Context ctx = Context.current().withValue(TENANT, headers.get(GrpcCalls.TENANT)).withValue(PEER, peer);
                return Contexts.interceptCall(ctx, call, headers, next);
            }
        };
    }

    static StatusRuntimeException error(Status status, String code) {
        Metadata t = new Metadata();
        t.put(GrpcCalls.PARITY_ERROR, code);
        return status.asRuntimeException(t);
    }

    /** INVALID_ARGUMENT/tenant_mismatch unless the metadata names the request's tenant. */
    static void checkTenant(String tenantId) {
        if (tenantId == null || !tenantId.equals(TENANT.get())) {
            throw error(Status.INVALID_ARGUMENT, "tenant_mismatch");
        }
    }

    static <T> void reply(StreamObserver<T> out, java.util.function.Supplier<T> body) {
        try {
            T value = body.get();
            out.onNext(value);
            out.onCompleted();
        } catch (StatusRuntimeException e) {
            out.onError(e);
        }
    }
}
