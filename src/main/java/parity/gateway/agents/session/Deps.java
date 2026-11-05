package parity.gateway.agents.session;

import java.util.List;
import java.util.concurrent.Executor;
import parity.gateway.agents.AgentsMetrics;
import parity.gateway.agents.auth.TokenVerifier;
import parity.gateway.agents.creds.SipCredentials;
import parity.gateway.agents.creds.TurnCredentials;
import parity.gateway.agents.ie.IeClient;
import parity.gateway.agents.re.ReClient;
import parity.gateway.config.GatewayProperties;

/** What every session uses. {@code closer}: platform threads for socket closes. */
public record Deps(GatewayProperties.Agents agents, String replicaId, ReClient re, IeClient ie, TokenVerifier tokens,
        TurnCredentials turn, SipCredentials sip, List<String> sipWsServers, AgentsMetrics metrics, Executor closer) {}
