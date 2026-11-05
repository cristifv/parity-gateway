package parity.gateway.lifecycle;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.springframework.stereotype.Component;

/**
 * The checks behind the healthy file. The {@code agents} module adds none that depend on RE, IE
 * or Kafka: agents keep their calls and sockets while those are down (PLAN.md, failure
 * behaviour), so the gateway stays healthy and the desktops show a banner instead.
 */
@Component
public class ReadinessChecks {

    /** A named check: {@code null} when it passes, else why not. */
    public record Check(String name, Supplier<String> failure) {}

    private final List<Check> checks;

    public ReadinessChecks(org.springframework.beans.factory.ObjectProvider<Check> checks) {
        this.checks = checks.orderedStream().toList();
    }

    public Map<String, String> evaluate() {
        Map<String, String> failures = new LinkedHashMap<>();
        for (Check check : checks) {
            String failure = check.failure().get();
            if (failure != null) {
                failures.put(check.name(), failure);
            }
        }
        return failures;
    }

    public List<String> names() {
        return checks.stream().map(Check::name).toList();
    }
}
