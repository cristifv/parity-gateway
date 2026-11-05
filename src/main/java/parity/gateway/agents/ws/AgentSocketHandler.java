package parity.gateway.agents.ws;

import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.server.ServerHttpRequest;
import org.springframework.http.server.ServerHttpResponse;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.adapter.NativeWebSocketSession;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;
import org.springframework.web.socket.server.HandshakeInterceptor;
import parity.gateway.agents.AgentsMetrics;
import parity.gateway.agents.auth.Identity;
import parity.gateway.agents.auth.TokenVerifier;
import parity.gateway.agents.session.AgentSession;
import parity.gateway.agents.session.SessionRegistry;

/**
 * {@code agent-ws.v1}'s socket: the first frame must be {@code auth} within the timeout (4008);
 * the token's tenant must be the host name's first label (4003); logins beyond this replica's
 * rate are told to retry later (4013 with {@code retry_after_ms}). Then frames go to the session.
 */
public final class AgentSocketHandler extends TextWebSocketHandler implements HandshakeInterceptor {

    private static final Logger log = LoggerFactory.getLogger(AgentSocketHandler.class);
    private static final String HOST_TENANT = "parity.host_tenant";
    private static final String SESSION = "parity.session";

    private final SessionRegistry sessions;
    private final TokenVerifier tokens;
    private final AgentsMetrics metrics;
    private final ScheduledExecutorService timer;
    private final java.util.List<String> hostHeaders;
    private final long authTimeoutMs;

    public AgentSocketHandler(SessionRegistry sessions, TokenVerifier tokens, AgentsMetrics metrics,
            ScheduledExecutorService timer, String hostHeader, long authTimeoutMs) {
        this.sessions = sessions;
        this.tokens = tokens;
        this.metrics = metrics;
        this.timer = timer;
        this.hostHeaders = java.util.regex.Pattern.compile(",").splitAsStream(hostHeader).map(String::strip)
                .filter(x -> !x.isEmpty()).toList();
        this.authTimeoutMs = authTimeoutMs;
    }

    /** The tenant named by the host: its first label, without a port. */
    static String hostTenant(String host) {
        if (host == null || host.isBlank()) {
            return null;
        }
        int comma = host.indexOf(',');
        String h = (comma >= 0 ? host.substring(0, comma) : host).trim().toLowerCase(java.util.Locale.ROOT);
        int colon = h.indexOf(':');
        if (colon >= 0) {
            h = h.substring(0, colon);
        }
        int dot = h.indexOf('.');
        return dot <= 0 ? null : h.substring(0, dot);
    }

    /**
     * The host the tenant is read from: the first of the configured headers (comma-separated,
     * in order) that the request carries. Production names only the proxy's {@code
     * x-forwarded-host}; the loopback-only dev profile also takes {@code host} (a browser, no
     * proxy: GW-033).
     */
    private String firstHost(ServerHttpRequest request) {
        for (String h : hostHeaders) {
            String v = request.getHeaders().getFirst(h);
            if (v != null && !v.isBlank()) {
                return v;
            }
        }
        return null;
    }

    @Override
    public boolean beforeHandshake(ServerHttpRequest request, ServerHttpResponse response, WebSocketHandler handler,
            Map<String, Object> attributes) {
        if (sessions.draining()) {
            response.setStatusCode(HttpStatus.SERVICE_UNAVAILABLE);
            return false;
        }
        String tenant = hostTenant(firstHost(request));
        if (tenant == null) {
            response.setStatusCode(HttpStatus.BAD_REQUEST);
            return false;
        }
        attributes.put(HOST_TENANT, tenant);
        return true;
    }

    @Override
    public void afterHandshake(ServerHttpRequest request, ServerHttpResponse response, WebSocketHandler handler,
            Exception exception) {
        // nothing
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession ws) {
        var unused = timer.schedule(() -> {
            if (ws.isOpen() && !ws.getAttributes().containsKey(SESSION)) {
                metrics.count("login_refused", "auth_timeout");
                refused(ws, "auth_timeout", 4008);
                close(ws, 4008, "auth_timeout");
            }
        }, authTimeoutMs, TimeUnit.MILLISECONDS);
    }

    @Override
    protected void handleTextMessage(WebSocketSession ws, TextMessage message) {
        Object s = ws.getAttributes().get(SESSION);
        if (s instanceof AgentSession session) {
            session.onText(message.getPayload());
            return;
        }
        Frames.Frame f;
        try {
            f = Frames.parse(message.getPayload());
        } catch (Frames.Invalid e) {
            refused(ws, "bad_frame", 4001);
            close(ws, 4001, "auth_failed");
            return;
        }
        if (!"auth".equals(f.type())) {
            refused(ws, "auth_first", 4001);
            close(ws, 4001, "auth_first");
            return;
        }
        Identity id;
        try {
            id = tokens.verify(f.text("token"));
        } catch (TokenVerifier.Refused e) {
            metrics.count("login_refused", e.code());
            refused(ws, e.code(), 4001);
            close(ws, 4001, "auth_failed");
            return;
        }
        if (!id.tenantId().equals(ws.getAttributes().get(HOST_TENANT))) {
            metrics.count("login_refused", "tenant_mismatch");
            refused(ws, "tenant_mismatch", 4003);
            close(ws, 4003, "tenant_mismatch");
            return;
        }
        if (!sessions.admit()) {
            metrics.count("login_refused", "busy");
            refused(ws, "busy", 4013);
            close(ws, 4013, "retry_after_ms=" + ThreadLocalRandom.current().nextLong(500, 3000));
            return;
        }
        jakarta.websocket.Session nativeSocket = ((NativeWebSocketSession) ws).getNativeSession(jakarta.websocket.Session.class);
        AgentSession session = sessions.open(id, ws, nativeSocket);
        if (session == null) {
            metrics.count("login_refused", "tenant_cap");
            refused(ws, "tenant_cap", 4013);
            close(ws, 4013, "retry_after_ms=" + ThreadLocalRandom.current().nextLong(5000, 15000));
            return;
        }
        ws.getAttributes().put(SESSION, session);
    }

    @Override
    public void afterConnectionClosed(WebSocketSession ws, CloseStatus status) {
        if (ws.getAttributes().get(SESSION) instanceof AgentSession session) {
            session.socketClosed(status.getCode());
        }
    }

    @Override
    public void handleTransportError(WebSocketSession ws, Throwable exception) {
        metrics.count("socket_error", exception.getClass().getSimpleName());
        log.atDebug().addKeyValue("event", "session.socket_error").addKeyValue("error_kind", exception.getClass().getName())
                .log("a desktop socket failed");
    }

    /** A login refused before a session exists (dedup: one record per reason and window). */
    private static void refused(WebSocketSession ws, String reason, int code) {
        Object tenant = ws.getAttributes().get(HOST_TENANT);
        log.atWarn().addKeyValue("event", "session.refused").addKeyValue("tenant_id", tenant).addKeyValue("reason", reason)
                .addKeyValue("close_code", code).log("a desktop login was refused");
    }

    private static void close(WebSocketSession ws, int code, String reason) {
        try {
            ws.close(new CloseStatus(code, reason));
        } catch (java.io.IOException | RuntimeException e) {
            // already gone
        }
    }
}
