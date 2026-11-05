package parity.gateway.agents.auth;

import java.time.Instant;

/** Who a desktop is, from its token only: never from a frame. */
public record Identity(String tenantId, String agentId, String sid, Instant expiresAt) {}
