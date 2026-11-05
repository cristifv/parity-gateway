package parity.gateway.agents;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Date;
import java.util.List;

/** Dev and test keys: a JWKS file the gateway trusts, tokens minted with it, a SIP key file. */
public final class TestKeys {

    public static final String ISSUER = "parity-gateway-dev";
    private final RSAKey key;
    public final Path dir;

    public TestKeys() throws Exception {
        dir = Files.createTempDirectory("gw-keys");
        key = new RSAKeyGenerator(2048).keyID("test").generate();
        Files.writeString(dir.resolve("jwks.json"), new JWKSet(key.toPublicJWK()).toString());
        Files.write(dir.resolve("sip.key"), "0123456789abcdef0123456789abcdef-sip-test-key".getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    public String jwks() {
        return dir.resolve("jwks.json").toString();
    }

    public String sipKey() {
        return dir.resolve("sip.key").toString();
    }

    public String token(String tenant, String agent) throws Exception {
        return token(tenant, agent, List.of("agent"), 600_000);
    }

    @SuppressWarnings("JavaUtilDate")
    public String token(String tenant, String agent, List<String> roles, long ttlMs) throws Exception {
        JWTClaimsSet c = new JWTClaimsSet.Builder().issuer(ISSUER).subject(agent)
                .claim("tenant_id", tenant).claim("agent_id", agent).claim("roles", roles).claim("sid", "sid-" + agent)
                .expirationTime(new Date(System.currentTimeMillis() + ttlMs)).build();
        SignedJWT jwt = new SignedJWT(new JWSHeader.Builder(JWSAlgorithm.RS256).keyID("test").build(), c);
        jwt.sign(new RSASSASigner(key));
        return jwt.serialize();
    }
}
