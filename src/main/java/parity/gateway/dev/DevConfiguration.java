package parity.gateway.dev;

import java.time.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import parity.gateway.config.GatewayProperties;

/** Profile {@code dev} (application-dev.yml): a local freeswitch registrar's static SIP passwords (GW-003). */
@Configuration(proxyBeanMethods = false)
@Profile("dev")
public class DevConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(DevConfiguration.class);

    @Bean
    DevSipPasswords devSipPasswords(GatewayProperties p, @Value("${parity.gateway.dev.sip-passwords-file}") String file,
            @Value("${server.address:}") String bindAddress) {
        DevSipPasswords d = DevSipPasswords.load(file, bindAddress, p.agents().sip().wsServers(), p.agents().sip().ttl(),
                Clock.systemUTC());
        LOG.atWarn().addKeyValue("event", "dev.sip_passwords_in_use").addKeyValue("agents", d.size())
                .log("dev SIP passwords in use (profile dev, loopback only, GW-003): never in production");
        return d;
    }
}
