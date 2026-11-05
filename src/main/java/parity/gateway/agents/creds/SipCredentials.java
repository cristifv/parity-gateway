package parity.gateway.agents.creds;

/**
 * What the softphone registers with: the {@code sip_credentials} frame's token. {@link SipTokens}
 * (the session-bound SIP JWT) everywhere; {@code DevSipPasswords} only in the {@code dev} profile
 * on loopback (GW-003).
 */
public interface SipCredentials {

    /** A token and when it expires. */
    record Grant(String token, long expiresAtMs) {}

    /** The grant for this session's agent, or null when the agent has none (no frame is sent). */
    Grant issue(String tenantId, String agentId, String sipUri, String sessionId);
}
