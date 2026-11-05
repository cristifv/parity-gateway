package parity.gateway;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.ConfigurableApplicationContext;
import parity.gateway.lifecycle.ExitCodes;
import parity.gateway.lifecycle.ShutdownSignals;

/** parity-gateway: one image, modules switched on per instance; v1 runs only {@code agents}. */
@SpringBootApplication
@ConfigurationPropertiesScan
public class ParityGatewayApplication {

    public static void main(String[] args) {
        // Before the start: a signal during it must not leave a healthy file or the JVM's 143.
        ShutdownSignals.install();
        ConfigurableApplicationContext context;
        try {
            context = SpringApplication.run(ParityGatewayApplication.class, args);
        } catch (Throwable failure) {
            System.exit(ExitCodes.forStartupFailure(failure));
            return;
        }
        ShutdownSignals.started(context);
    }
}
