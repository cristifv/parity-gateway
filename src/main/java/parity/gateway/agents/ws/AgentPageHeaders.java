package parity.gateway.agents.ws;

import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.URI;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import parity.gateway.lifecycle.ConfigurationRefusedException;

/**
 * The agent page's response headers (PLAN "the agent page": strict CSP, no third-party script).
 * The CSP is a header, not a meta tag, so {@code connect-src} names exactly the configured SIP
 * WebSocket servers' origins (opensips in production; a local freeswitch in dev) next to
 * {@code 'self'} (the gateway's own WebSocket, ws or wss with the page's scheme).
 */
public final class AgentPageHeaders implements Filter {

    private static final Pattern ORIGIN = Pattern.compile("^wss?://[A-Za-z0-9.-]{1,253}(:[0-9]{1,5})?$");

    private final String csp;

    public AgentPageHeaders(List<String> sipWsServers) {
        Set<String> origins = new LinkedHashSet<>();
        for (String s : sipWsServers) {
            String origin;
            try {
                URI u = new URI(s);
                origin = u.getScheme() + "://" + u.getHost() + (u.getPort() < 0 ? "" : ":" + u.getPort());
            } catch (java.net.URISyntaxException | RuntimeException e) {
                throw new ConfigurationRefusedException("a sip.ws-servers entry is not a ws or wss URL");
            }
            if (!ORIGIN.matcher(origin).matches()) {
                throw new ConfigurationRefusedException("a sip.ws-servers entry is not a ws or wss URL");
            }
            origins.add(origin);
        }
        this.csp = "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self'; media-src 'self' blob:; "
                + "connect-src 'self'" + (origins.isEmpty() ? "" : " " + String.join(" ", origins)) + "; "
                + "frame-ancestors 'none'; base-uri 'none'; form-action 'none'; object-src 'none'";
    }

    public String csp() {
        return csp;
    }

    @Override
    public void doFilter(ServletRequest req, ServletResponse res, FilterChain chain) throws IOException, ServletException {
        if (res instanceof HttpServletResponse h) {
            h.setHeader("Content-Security-Policy", csp);
            h.setHeader("X-Content-Type-Options", "nosniff");
            h.setHeader("Referrer-Policy", "no-referrer");
            h.setHeader("Cache-Control", "no-cache");
        }
        chain.doFilter(req, res);
    }
}
