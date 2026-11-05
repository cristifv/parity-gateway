package parity.gateway.agents.ie;

import io.grpc.ManagedChannel;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import parity.contracts.interaction.v1.ExecuteRequest;
import parity.contracts.interaction.v1.ExecuteResponse;
import parity.contracts.interaction.v1.GetRequest;
import parity.contracts.interaction.v1.GetResponse;
import parity.contracts.interaction.v1.InteractionControlServiceGrpc;
import parity.gateway.agents.GrpcCalls;
import parity.gateway.config.GatewayProperties.Endpoint;

/**
 * IE's {@code InteractionControlService}: 2 s in total; {@code UNAVAILABLE} (owner_moving: after
 * 1 s) and {@code INTERNAL} retried with the same command_id.
 */
public class IeClient implements AutoCloseable {

    private static final Duration BUDGET = Duration.ofSeconds(2);

    private final ManagedChannel channel;
    private final String peer;
    private final boolean testPeer;

    public IeClient(Endpoint endpoint, String peer) {
        this.channel = GrpcCalls.channel(endpoint);
        this.peer = peer;
        this.testPeer = endpoint.testPeer();
    }

    private InteractionControlServiceGrpc.InteractionControlServiceBlockingStub stub(String tenant, Duration d) {
        return InteractionControlServiceGrpc.newBlockingStub(GrpcCalls.with(channel, tenant, peer, testPeer))
                .withDeadlineAfter(d.toNanos(), TimeUnit.NANOSECONDS);
    }

    public ExecuteResponse execute(ExecuteRequest r) throws GrpcCalls.Failed {
        return GrpcCalls.call(d -> stub(r.getTenantId(), d).execute(r), 3, BUDGET, Duration.ofMillis(50), BUDGET);
    }

    public GetResponse get(GetRequest r) throws GrpcCalls.Failed {
        return GrpcCalls.call(d -> stub(r.getTenantId(), d).get(r), 3, BUDGET, Duration.ofMillis(50), BUDGET);
    }

    @Override
    public void close() {
        channel.shutdownNow();
    }
}
