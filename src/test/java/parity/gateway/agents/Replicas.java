package parity.gateway.agents;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import parity.gateway.ParityGatewayApplication;
import parity.gateway.fake.FakeInteractionEngine;
import parity.gateway.fake.FakeRoutingEngine;

/** Gateway replicas in this JVM, pointed at a fake RE and a fake IE, with test keys. */
final class Replicas {

    private Replicas() {}

    static ConfigurableApplicationContext start(String instance, FakeRoutingEngine re, FakeInteractionEngine ie,
            TestKeys keys, Map<String, Object> extra) throws Exception {
        Path tmp = Files.createTempDirectory("gw-" + instance);
        Map<String, Object> p = new HashMap<>();
        p.put("server.port", 0);
        p.put("management.server.port", -1);
        p.put("parity.gateway.instance", instance);
        p.put("parity.gateway.lifecycle.healthy-file", tmp.resolve("healthy").toString());
        p.put("parity.gateway.lifecycle.drained-file", tmp.resolve("drained").toString());
        p.put("parity.gateway.lifecycle.deregistration-delay", "0s");
        p.put("parity.gateway.lifecycle.drain-timeout", "20s");
        p.put("parity.gateway.agents.re.port", re.port());
        p.put("parity.gateway.agents.re.test-peer", true);
        p.put("parity.gateway.agents.ie.port", ie.port());
        p.put("parity.gateway.agents.ie.test-peer", true);
        p.put("parity.gateway.agents.events.enabled", false);
        p.put("parity.gateway.agents.auth.jwks-file", keys.jwks());
        p.put("parity.gateway.agents.auth.issuer", TestKeys.ISSUER);
        p.put("parity.gateway.agents.sip.key-file", keys.sipKey());
        p.put("parity.gateway.agents.sip.ws-servers[0]", "wss://sip.acme.eu1.platform.example");
        p.put("parity.gateway.agents.turn.secret-file", "");
        p.put("parity.gateway.agents.turn.urls[0]", "stun:stun.eu1.platform.example:3478");
        p.put("parity.gateway.agents.auth-timeout", "1s");
        p.putAll(extra);
        // Arguments, not default properties: application.yml would win over those.
        String[] args = p.entrySet().stream().map(e -> "--" + e.getKey() + "=" + e.getValue()).toArray(String[]::new);
        return new SpringApplicationBuilder(ParityGatewayApplication.class).run(args);
    }

    static int port(ConfigurableApplicationContext ctx) {
        return Integer.parseInt(ctx.getEnvironment().getProperty("local.server.port"));
    }
}
