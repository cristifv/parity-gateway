package parity.gateway.fake;

import java.io.IOException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/** Profile {@code fake}: the fake RE and fake IE in this process (IE step 5 can use the RE half). */
@Configuration(proxyBeanMethods = false)
@Profile("fake")
public class FakeServers {

    @Bean(destroyMethod = "close")
    FakeRoutingEngine fakeRoutingEngine(@Value("${parity.gateway.fake.re-port}") int port) throws IOException {
        return new FakeRoutingEngine(System::currentTimeMillis, 15_000, 60_000).start(port, true);
    }

    @Bean(destroyMethod = "close")
    FakeInteractionEngine fakeInteractionEngine(@Value("${parity.gateway.fake.ie-port}") int port) throws IOException {
        return new FakeInteractionEngine().start(port);
    }
}
