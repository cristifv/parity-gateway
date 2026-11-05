package parity.gateway.agents;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.function.Supplier;

/** The {@code agents} module's meters (PLAN.md, observability). No tenant label. */
public final class AgentsMetrics {

    private final MeterRegistry registry;
    private final Counter offers;

    public AgentsMetrics(MeterRegistry registry) {
        this.registry = registry;
        this.offers = registry.counter("parity_gateway_agents_offers_delivered");
    }

    /** The sessions gauge, registered once by the session registry's owner. */
    public void sessions(Supplier<Number> sessions) {
        registry.gauge("parity_gateway_agents_sessions", sessions, s -> s.get().doubleValue());
    }

    public void offerDelivered() {
        offers.increment();
    }

    /** {@code parity_gateway_agents_events{what, code}}: refusals, failures, drops. */
    public void count(String what, String code) {
        registry.counter("parity_gateway_agents_events", "what", what, "code", code == null ? "none" : code).increment();
    }
}
