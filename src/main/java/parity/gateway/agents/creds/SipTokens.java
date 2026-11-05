package parity.gateway.agents.creds;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.MACSigner;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.Date;
import java.util.UUID;

/**
 * The softphone's short-lived SIP token (SAML-PLAN 12), checked by opensips {@code auth_jwt} on
 * REGISTER: HS256 with the gateway's own key, so it keeps being renewed while Keycloak is down.
 * Bound to the desktop session ({@code sid} = the gateway's session id, GW-009): a superseded
 * session's token is not renewed. No static credentials exist (GW-003).
 */
public final class SipTokens implements SipCredentials {

    private final MACSigner signer;
    private final Duration ttl;
    private final Clock clock;

    public SipTokens(String keyFile, Duration ttl, Clock clock) {
        try {
            this.signer = new MACSigner(Files.readAllBytes(Path.of(keyFile)));
        } catch (java.io.IOException | JOSEException e) {
            throw new IllegalStateException("the SIP key file is missing or shorter than 32 bytes", e);
        }
        this.ttl = ttl;
        this.clock = clock;
    }

    @Override
    @SuppressWarnings("JavaUtilDate") // nimbus takes java.util.Date
    public Grant issue(String tenantId, String agentId, String sipUri, String sessionId) {
        long now = clock.millis();
        long exp = now + ttl.toMillis();
        JWTClaimsSet claims = new JWTClaimsSet.Builder()
                .issuer("parity-gateway").subject(agentId).audience("opensips")
                .claim("tenant_id", tenantId).claim("sip_uri", sipUri).claim("sid", sessionId)
                .jwtID(UUID.randomUUID().toString())
                .issueTime(new Date(now)).expirationTime(new Date(exp)).build();
        SignedJWT jwt = new SignedJWT(new JWSHeader(JWSAlgorithm.HS256), claims);
        try {
            jwt.sign(signer);
        } catch (JOSEException e) {
            throw new IllegalStateException("can't sign the SIP token", e);
        }
        return new Grant(jwt.serialize(), exp);
    }
}
