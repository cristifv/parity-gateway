package parity.gateway.agents.auth;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.source.ImmutableJWKSet;
import com.nimbusds.jose.proc.JWSVerificationKeySelector;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.proc.DefaultJWTClaimsVerifier;
import com.nimbusds.jwt.proc.DefaultJWTProcessor;
import java.io.File;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Checks a desktop's JWT locally against a JWKS file (Keycloak's realm keys later; dev and test
 * keys now: the same code path). Required: {@code iss}, {@code exp}, {@code tenant_id} (a DNS
 * label), {@code agent_id} (opaque, never an email), {@code roles} containing {@code agent}.
 */
public final class TokenVerifier {

    /** A refused token; the message is a constant reason code. */
    public static final class Refused extends Exception {
        private static final long serialVersionUID = 1L;
        private final String code;

        public Refused(String code) {
            super("token refused", null, false, false);
            this.code = code;
        }

        public String code() {
            return code;
        }
    }

    static final Pattern TENANT = Pattern.compile("^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$");
    static final Pattern AGENT = Pattern.compile("^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$");

    private final DefaultJWTProcessor<SecurityContext> processor = new DefaultJWTProcessor<>();

    public TokenVerifier(String jwksFile, String issuer) {
        JWKSet keys;
        try {
            keys = JWKSet.load(new File(jwksFile));
        } catch (Exception e) {
            throw new IllegalStateException("can't read the JWKS file", e);
        }
        processor.setJWSKeySelector(new JWSVerificationKeySelector<>(
                Set.of(JWSAlgorithm.RS256, JWSAlgorithm.ES256), new ImmutableJWKSet<>(keys)));
        DefaultJWTClaimsVerifier<SecurityContext> claims = new DefaultJWTClaimsVerifier<>(
                new JWTClaimsSet.Builder().issuer(issuer).build(),
                Set.of("exp", "tenant_id", "agent_id", "roles"));
        claims.setMaxClockSkew(30);
        processor.setJWTClaimsSetVerifier(claims);
    }

    public Identity verify(String token) throws Refused {
        JWTClaimsSet c;
        try {
            c = processor.process(token, null);
        } catch (Exception e) {
            throw new Refused("token_invalid");
        }
        try {
            String tenant = c.getStringClaim("tenant_id");
            String agent = c.getStringClaim("agent_id");
            List<String> roles = c.getStringListClaim("roles");
            if (tenant == null || !TENANT.matcher(tenant).matches() || agent == null
                    || !AGENT.matcher(agent).matches() || agent.contains("@")) {
                throw new Refused("token_claims");
            }
            if (roles == null || !roles.contains("agent")) {
                throw new Refused("token_role");
            }
            String sid = c.getStringClaim("sid");
            return new Identity(tenant, agent, sid == null ? "" : sid, c.getExpirationTime().toInstant());
        } catch (java.text.ParseException e) {
            throw new Refused("token_claims");
        }
    }
}
