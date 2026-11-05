package parity.gateway.agents;

import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;
import org.springframework.web.socket.server.standard.ServletServerContainerFactoryBean;
import org.springframework.web.socket.server.support.DefaultHandshakeHandler;
import parity.gateway.agents.auth.TokenVerifier;
import parity.gateway.agents.creds.SipTokens;
import parity.gateway.dev.DevSipPasswords;
import parity.gateway.agents.creds.TurnCredentials;
import parity.gateway.agents.ie.EventsConsumer;
import parity.gateway.agents.ie.IeClient;
import parity.gateway.agents.re.AgentWatch;
import parity.gateway.agents.re.ReClient;
import parity.gateway.agents.session.Deps;
import parity.gateway.agents.session.SessionRegistry;
import parity.gateway.agents.ws.AgentPageHeaders;
import parity.gateway.agents.ws.AgentSocketHandler;
import parity.gateway.agents.ws.Frames;
import parity.gateway.config.GatewayProperties;

/** The {@code agents} module (v1), switched on by {@code parity.gateway.modules.agents.enabled}. */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "parity.gateway.modules.agents.enabled", havingValue = "true")
@EnableWebSocket
public class AgentsConfiguration {

    @Bean(destroyMethod = "shutdownNow")
    ScheduledExecutorService agentsTimer() {
        return Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().name("agents-timer").daemon(true).factory());
    }

    @Bean(destroyMethod = "shutdownNow")
    ExecutorService agentsCloser() {
        return Executors.newFixedThreadPool(4, Thread.ofPlatform().name("ws-close-", 0).daemon(true).factory());
    }

    @Bean(destroyMethod = "close")
    ReClient reClient(GatewayProperties p) {
        return new ReClient(p.agents().re(), "parity-gateway." + p.bin(), p.agents().attachConcurrency());
    }

    @Bean(destroyMethod = "close")
    IeClient ieClient(GatewayProperties p) {
        return new IeClient(p.agents().ie(), "parity-gateway." + p.bin());
    }

    @Bean
    TokenVerifier tokenVerifier(GatewayProperties p) {
        return new TokenVerifier(p.agents().auth().jwksFile(), p.agents().auth().issuer());
    }

    @Bean
    AgentsMetrics agentsMetrics(MeterRegistry meters) {
        return new AgentsMetrics(meters);
    }

    @Bean
    SessionRegistry sessionRegistry(GatewayProperties p, ReClient re, IeClient ie, TokenVerifier tokens,
            AgentsMetrics metrics, ExecutorService agentsCloser, ScheduledExecutorService agentsTimer,
            ObjectProvider<DevSipPasswords> devSip) {
        var a = p.agents();
        // The SIP JWT, except in the dev profile on loopback (GW-003): a local freeswitch's passwords.
        DevSipPasswords dev = devSip.getIfAvailable();
        Deps deps = new Deps(a, p.instance(), re, ie, tokens,
                new TurnCredentials(a.turn().secretFile(), a.turn().urls(), a.turn().ttl(), Clock.systemUTC()),
                dev != null ? dev : new SipTokens(a.sip().keyFile(), a.sip().ttl(), Clock.systemUTC()),
                a.sip().wsServers(), metrics, agentsCloser);
        SessionRegistry registry = new SessionRegistry(deps);
        metrics.sessions(registry::size);
        long every = a.pingInterval().toMillis();
        var unused = agentsTimer.scheduleAtFixedRate(registry::tickAll, every, every, TimeUnit.MILLISECONDS);
        return registry;
    }

    @Bean(initMethod = "start", destroyMethod = "close")
    AgentWatch agentWatch(GatewayProperties p, ReClient re, SessionRegistry sessions) {
        return new AgentWatch(re, sessions, p.instance(), p.bin());
    }

    @Bean(destroyMethod = "close")
    EventsConsumer eventsConsumer(GatewayProperties p, SessionRegistry sessions, AgentsMetrics metrics) {
        var e = p.agents().events();
        EventsConsumer c = new EventsConsumer(e.bootstrapServers(), e.topic(), p.instance(), sessions, metrics);
        if (e.enabled()) {
            c.start();
        }
        return c;
    }

    @Bean
    AgentSocketHandler agentSocketHandler(GatewayProperties p, SessionRegistry sessions, TokenVerifier tokens,
            AgentsMetrics metrics, ScheduledExecutorService agentsTimer) {
        return new AgentSocketHandler(sessions, tokens, metrics, agentsTimer,
                p.agents().hostHeader(), p.agents().authTimeout().toMillis());
    }

    @Bean
    FilterRegistrationBean<AgentPageHeaders> agentPageHeaders(GatewayProperties p) {
        FilterRegistrationBean<AgentPageHeaders> r = new FilterRegistrationBean<>(new AgentPageHeaders(p.agents().sip().wsServers()));
        r.addUrlPatterns("/agent/*");
        return r;
    }

    @Bean
    ServletServerContainerFactoryBean websocketContainer() {
        ServletServerContainerFactoryBean c = new ServletServerContainerFactoryBean();
        c.setMaxTextMessageBufferSize(Frames.MAX_BYTES);
        c.setMaxSessionIdleTimeout(60_000L);
        return c;
    }

    @Bean
    WebSocketConfigurer agentsSocket(GatewayProperties p, AgentSocketHandler handler) {
        return (WebSocketHandlerRegistry registry) -> {
            DefaultHandshakeHandler handshake = new DefaultHandshakeHandler();
            handshake.setSupportedProtocols("parity.agent.v1");
            registry.addHandler(handler, p.agents().path()).setHandshakeHandler(handshake)
                    .addInterceptors(handler).setAllowedOriginPatterns("*");
        };
    }
}
