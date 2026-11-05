package parity.gateway.agents.re;

import io.grpc.ManagedChannel;
import java.time.Duration;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import parity.contracts.agents.v1.AgentServiceGrpc;
import parity.contracts.agents.v1.AnswerOfferRequest;
import parity.contracts.agents.v1.AnswerOfferResponse;
import parity.contracts.agents.v1.AttachAgentRequest;
import parity.contracts.agents.v1.AttachAgentResponse;
import parity.contracts.agents.v1.DetachAgentRequest;
import parity.contracts.agents.v1.DetachAgentResponse;
import parity.contracts.agents.v1.EndWrapUpRequest;
import parity.contracts.agents.v1.EndWrapUpResponse;
import parity.contracts.agents.v1.SetStateRequest;
import parity.contracts.agents.v1.SetStateResponse;
import parity.gateway.agents.GrpcCalls;
import parity.gateway.config.GatewayProperties.Endpoint;

/**
 * RE's {@code AgentService} (parity-contracts CONVENTIONS.md, "The agents contract"): 300 ms per
 * try (attach 500 ms), up to 3 tries from 50 ms with full jitter, the same request (so the same
 * command_id). {@code AttachAgent} goes through a bulkhead so a login burst doesn't flood RE.
 */
public class ReClient implements AutoCloseable {

    private static final Duration TRY = Duration.ofMillis(300);
    private static final Duration ATTACH_TRY = Duration.ofMillis(500);
    private static final Duration BACKOFF = Duration.ofMillis(50);

    private final ManagedChannel channel;
    private final String peer;
    private final boolean testPeer;
    private final Semaphore attaches;

    public ReClient(Endpoint endpoint, String peer, int attachConcurrency) {
        this.channel = GrpcCalls.channel(endpoint);
        this.peer = peer;
        this.testPeer = endpoint.testPeer();
        this.attaches = new Semaphore(attachConcurrency);
    }

    ManagedChannel channel() {
        return channel;
    }

    String peer() {
        return peer;
    }

    boolean testPeer() {
        return testPeer;
    }

    private AgentServiceGrpc.AgentServiceBlockingStub stub(String tenant, Duration deadline) {
        return AgentServiceGrpc.newBlockingStub(GrpcCalls.with(channel, tenant, peer, testPeer))
                .withDeadlineAfter(deadline.toNanos(), TimeUnit.NANOSECONDS);
    }

    public AttachAgentResponse attach(AttachAgentRequest r) throws GrpcCalls.Failed {
        try {
            if (!attaches.tryAcquire(2, TimeUnit.SECONDS)) {
                throw new GrpcCalls.Failed(io.grpc.Status.Code.UNAVAILABLE, "unavailable");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new GrpcCalls.Failed(io.grpc.Status.Code.UNAVAILABLE, "unavailable");
        }
        try {
            return GrpcCalls.call(d -> stub(r.getTenantId(), d).attachAgent(r), 3, ATTACH_TRY, BACKOFF, Duration.ofSeconds(2));
        } finally {
            attaches.release();
        }
    }

    public DetachAgentResponse detach(DetachAgentRequest r) throws GrpcCalls.Failed {
        return GrpcCalls.call(d -> stub(r.getSession().getTenantId(), d).detachAgent(r), 3, TRY, BACKOFF, Duration.ofSeconds(1));
    }

    public SetStateResponse setState(SetStateRequest r) throws GrpcCalls.Failed {
        return GrpcCalls.call(d -> stub(r.getSession().getTenantId(), d).setState(r), 3, TRY, BACKOFF, Duration.ofSeconds(1));
    }

    public AnswerOfferResponse answerOffer(AnswerOfferRequest r) throws GrpcCalls.Failed {
        return GrpcCalls.call(d -> stub(r.getSession().getTenantId(), d).answerOffer(r), 3, TRY, BACKOFF, Duration.ofSeconds(1));
    }

    public EndWrapUpResponse endWrapUp(EndWrapUpRequest r) throws GrpcCalls.Failed {
        return GrpcCalls.call(d -> stub(r.getSession().getTenantId(), d).endWrapUp(r), 3, TRY, BACKOFF, Duration.ofSeconds(1));
    }

    @Override
    public void close() {
        channel.shutdownNow();
    }
}
