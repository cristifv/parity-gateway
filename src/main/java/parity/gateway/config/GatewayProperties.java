package parity.gateway.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.time.Duration;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/** {@code parity.gateway.*} (application.yml). */
@Validated
@ConfigurationProperties("parity.gateway")
public record GatewayProperties(
        @NotBlank String zone,
        @NotBlank String bin,
        @NotBlank String instance,
        String version,
        @Valid @NotNull Lifecycle lifecycle,
        @Valid @NotNull Readiness readiness,
        @Valid @NotNull Modules modules,
        @Valid @NotNull Agents agents) {

    /** The healthy and drained files and the drain's timings (ARCH §7.3). */
    public record Lifecycle(@NotBlank String healthyFile, @NotBlank String drainedFile,
            @NotNull Duration deregistrationDelay, @NotNull Duration drainTimeout) {}

    /** How often the readiness gate looks. */
    public record Readiness(@NotNull Duration pollInterval) {}

    /** Which modules run in this instance. */
    public record Modules(@Valid @NotNull Module agents) {}

    /** One module's switch. */
    public record Module(boolean enabled) {}

    /** A gRPC peer. {@code testPeer}: send x-parity-test-peer (fakes and tests only, GW-012). */
    public record Endpoint(@NotBlank String host, @Min(1) int port, boolean testPeer) {}

    /** The {@code agents} module. */
    public record Agents(
            @NotBlank String path,
            // The header that names the tenant: host, or x-forwarded-host behind Traefik.
            @NotBlank String hostHeader,
            // An agent's contact is sip:{agent}@{tenant}.{sipDomain}.
            @NotBlank String sipDomain,
            @NotNull Duration authTimeout,
            @NotNull Duration pingInterval,
            @NotNull Duration drainSpread,
            @Min(1) int admissionPerSecond,
            @Min(1) int commandsPerSecond,
            @Min(1) int maxSessionsPerTenant,
            @Min(1) int attachConcurrency,
            @Min(1) int outboxLimit,
            @Valid @NotNull Endpoint re,
            @Valid @NotNull Endpoint ie,
            @Valid @NotNull Events events,
            @Valid @NotNull Auth auth,
            @Valid @NotNull Turn turn,
            @Valid @NotNull Sip sip) {}

    /** interaction.events (read only). */
    public record Events(boolean enabled, String bootstrapServers, @NotBlank String topic) {}

    /** Desktop tokens: a JWKS file (Keycloak's keys, or dev and test keys) and the issuer. */
    public record Auth(@NotBlank String jwksFile, @NotBlank String issuer) {}

    /** TURN REST credentials; no secret file: STUN only. */
    public record Turn(String secretFile, @NotNull List<String> urls, @NotNull Duration ttl) {}

    /** The session-bound SIP token (opensips auth_jwt, SAML-PLAN 12; GW-003, GW-009). */
    public record Sip(@NotBlank String keyFile, @NotNull List<String> wsServers, @NotNull Duration ttl) {}
}
