# parity-gateway: one gateway service

parity-gateway is **one service for every gateway** the platform needs. It takes over five
roles that the architecture docs give to separate services or leave unassigned:

| Module | Takes over | In the docs |
|---|---|---|
| `channels` | channel-gateway: web chat, SMS, WhatsApp, Messenger, Instagram and email | `ARCHITECTURE.md` §3.12 |
| `agents` | agent-gateway: the agent desktop's WebSocket, offers, call controls, TURN credentials | §3.8 |
| `llm` | the LLM gateway: every AI model call (opt-in, residency, redaction, metering, tools) | §3.18, inside agentanalytics |
| `push` | push-gateway: APNs and FCM pushes that wake mobile softphones | `UCAAS.md` |
| `api` | new: HTTPS in front of the parity services' Kafka APIs | not in the docs |

**Status:** plan only, no code yet (2026-10-17). This file holds the assessment of the
architecture docs for a single gateway, and the plan. Everything below marked *proposed*
is open to change.

## Requirements so far

From the user:
1. A new service in its own directory in `~/Repos`, with a plan.
2. **"A single gateway service that handles all of those that were mentioned":**
   channel-gateway, agent-gateway, the LLM gateway, push-gateway, and an API gateway in front
   of the parity services' Kafka APIs.

From the platform's standing rules:
- Java services use Maven, Spring Boot and Lombok, in annotation style.
- Every service creates `/tmp/<service>.healthy` and `/tmp/<service>.drained` (§7.3).
- The message conventions of the other parity services (`cmd`, `id`, `reply_to`, `ok`).

## Assessment of the architecture for one gateway

### What the docs say about each

- **channel-gateway (§3.12):**
  - one service already, with one connector per digital channel;
  - the same pipeline for every connector. **In:** verify the signature or session token,
    turn the payload into a common `Message`, drop duplicates by the provider's message ID,
    virus-scan and store attachments, hand the message to interaction-engine. **Out:** apply
    the channel's rules (WhatsApp 24-hour window, SMS STOP, email threading and DKIM), send,
    track delivery;
  - **connectors are isolated:** each has its own thread pool, queue, circuit breaker and
    rate limiter, and a feature flag per tenant;
  - per bin. Shared provider connections (Meta and aggregator webhooks, SMPP binds, inbound
    email) are held by the live bin, which hands messages to the tenant's bin through a
    Postgres inbox and outbox (§7.8);
  - phase 4, one connector at a time: web chat → email → WhatsApp → the rest (§9).
- **agent-gateway (§3.8, §6.5):**
  - the agent desktop's WebSocket: state, offers on every channel, accept and reject, call
    controls, message sending, all relayed to interaction-engine and routing-engine;
  - also the assist panel of agentanalytics;
  - time-limited TURN credentials for coturn;
  - any replica serves any agent (agent events come from Redpanda, sessions are cached in
    Redis);
  - **on the v1 call path:** opensips → freeswitch + parity-ms → interaction-engine →
    routing-engine → agent-gateway (§9.1).
- **LLM gateway (§3.18):** inside agentanalytics, every model call goes through it:
  - tenant opt-in and per-feature switches;
  - the provider chosen by the tenant's residency (Claude API, Bedrock in the EU,
    zero data retention, or a self-hosted model);
  - card numbers, IBANs and other personal data removed before a prompt is sent;
  - versioned prompts, prompt caching, cost metered per tenant to billing;
  - the MCP client of parity-context-mcp, offering the model only the tools of the person it
    acts for.
- **push-gateway (`UCAAS.md`):** "a small push-gateway that calls APNs and FCM directly". It
  works with opensips' RFC 8599 push: when a call comes for a mobile that is asleep, the
  device is woken by a push so it registers and takes the call. UCaaS is a proposal; the
  mobile app doesn't exist.
- **An API gateway:** not in the docs. Traefik is the HTTP ingress (TLS, routing to bins),
  and Cloudflare is in front of it. Every built parity service has a Kafka request/response
  API, and nothing turns HTTP into those requests. The admin-console plan
  (`../parity-admin-console/README.md`) has to build that itself.

### What exists for it to talk to

| Service | State | What the gateway can do with it now |
|---|---|---|
| parity-signal-detection | Built | Already reads `channel.messages`, "from channel-gateway". The `channels` module can feed it today |
| parity-customer-context | Built | Sends `consultation.requested` to "the agent or the supervisor"; nobody shows it to agents yet. It also calls Claude itself, and could call through the `llm` module |
| parity-rs, parity-geoip, parity-customer-context | Built, Kafka APIs | What the `api` module puts behind HTTPS |
| parity-ms | Built | Terminates the agents' WebRTC media; the gateway never carries media |
| interaction-engine, routing-engine | Not built | Where messages go and offers come from. Until they exist, `channels` publishes messages to Kafka, and `agents` has no offers to show |
| parity-context-mcp, agentanalytics, billing | Not built | The `llm` module's tools, its callers, and its cost metering |
| keycloak, coturn | Off the shelf | Agent JWTs; the shared secret for TURN credentials |

### Findings

1. **Merging has a precedent.** channel-gateway is already "one service, one connector per
   channel, isolated inside". The same pattern stretches to the other gateways: each becomes
   a module with its own threads, queue, circuit breaker and switch.
2. **They share a lot:**
   - JWT checks and the `tenant_id` claim (agents, the API, the chat widget's session tokens);
   - WebSockets with reconnect and resume (agent desktops and chat widgets), drained the
     same way: tell clients to reconnect with 0–10 s of jitter (§7.3);
   - finding the tenant (from a number, page, inbox, hostname or token);
   - per-tenant and per-provider rate limits as local token buckets (§5.1);
   - outbound HTTPS to outside providers through the egress-proxy allowlist (Meta, SMS
     aggregators, Claude or Bedrock, APNs, FCM; §7.7);
   - Kafka in and out, and a Postgres inbox, outbox and duplicate check.
3. **It puts four kinds of trust in one process:**
   - anonymous internet traffic (chat visitors, provider webhooks);
   - signed-in employees (agents);
   - internal callers (other services);
   - secrets that matter (LLM keys, APNs and FCM keys, provider tokens, the TURN secret).

   A bug in parsing a webhook could reach agent sessions and model keys. *Proposed:* every
   module can be switched on or off per instance. Development and small deployments run all
   of them in one process. Production can run the same image as separate deployments (for
   example `channels`, `agents` and `llm` apart) without a code change.
4. **The loads are very different.** About 10,000 long-lived agent WebSockets per zone (§1);
   bursts of webhooks; model calls that take seconds (live assist) to minutes (auto-QA);
   tiny pushes. In one JVM, a webhook storm or a backlog of model calls must not delay an
   agent's offer. Each module gets its own bounded pools and queues (bulkheads), and the
   `agents` module is measured on offer delivery latency (§7.5).
5. **The call path gets coupled to everything else.** agent-gateway is on the v1 call
   path. In one service, a bad connector or LLM release ships in the same image as the
   agents' connection. Bins limit the damage, because a tenant moves back to the previous
   release in seconds (§7.8). §10's risk "one gateway for every digital channel: a bad
   release could affect every channel at once" now covers agents and AI too.
6. **The roles live in different places today:**
   - channel-gateway and agent-gateway are per bin;
   - the LLM gateway is inside agentanalytics, also per bin;
   - push-gateway's place isn't given;
   - an API gateway in front of global services (parity-rs) and zone services
     (customer-context) has to reach both.

   *Proposed:* the gateway runs per bin like channel-gateway. Connections shared by every
   tenant (Meta webhooks, SMPP, inbound email, APNs and FCM) are held by the live bin's
   instances only. v1 is one zone, so the global question can wait.
7. **The LLM gateway can't be fed by Kafka alone.** Live assist needs streamed answers in
   about 1–2 s (§3.18), and Kafka isn't made for streaming tokens. *Proposed:* HTTP with
   server-sent events inside the zone for live calls, Kafka for batch work (summaries,
   auto-QA). customer-context's `AiAgent` could call it in place of the Anthropic API.
8. **Most modules wait on services that aren't built:**
   - `channels` hands messages to interaction-engine;
   - `agents` relays offers from routing-engine and call controls to interaction-engine;
   - `llm` serves agentanalytics and needs parity-context-mcp for tools;
   - `push` needs a mobile app and opensips' RFC 8599 setup.

   What works now: `api` over the built services; `channels` feeding signal-detection's
   `channel.messages`; `agents` with sessions, TURN credentials, and customer-context's
   consultations for agents; `llm` for customer-context.
9. **The API gateway sits next to Traefik, not in its place.** Traefik keeps TLS and routing
   to the tenant's bin; Cloudflare keeps the WAF. The `api` module is a service behind them
   that turns HTTPS requests into Kafka requests, checks who may send which command for
   which tenant, and waits for the answer.
10. **The Kafka APIs have no authentication** (also finding 6 of the admin-console plan).
    Once the `api` module checks roles, the request topics must be writable only by it and
    the named services (Kafka ACLs), or the check can be bypassed.
11. **The `Message` event has no topic yet.** The docs say channel-gateway "hands the event to
    the Interaction Engine" and also publishes to Redpanda for signal-detection, analytics
    and experience. signal-detection already defines `channel.messages`. *Proposed:* one
    topic, `channel.messages`, keyed by interaction id, with signal-detection's fields as the
    minimum. interaction-engine reads it when it exists.
12. **Push and UCaaS are undecided.** `ASSESSMENT.md` leaves open whether UCaaS starts with
    Teams or a native app, and whether the mobile app is built or licensed. The `push`
    module waits for that.

### Verdict

One gateway works if it's **one codebase and one image with isolated modules**, not one
shared pool of threads. channel-gateway's own design (isolated connectors with their own
pools, breakers and switches) scales up to five modules. Being able to run modules as
separate deployments keeps the trust and load risks (findings 3–5) manageable.

Build the parts that work without unbuilt services first: the `api` module, the web chat
connector feeding signal-detection, and agent sessions with TURN credentials.

## The plan

### Shape (proposed)

```
                           Cloudflare → L4 NLB → traefik
        ┌──────────────────────────┬───────────────────────────┬───────────────────────┐
  chat widget, webhooks      agent desktops (WSS)       admin-console, services    opensips (RFC 8599)
        │                          │                           │                       │
┌───────▼──────────────────────────▼───────────────────────────▼───────────────────────▼───┐
│ parity-gateway        channels │ agents │ api │ llm │ push   (each: own pools, breaker,  │
│                                                              queue, on/off switch)      │
└───────┬─────────────────────────┬──────────────┬─────────────┬──────────────────┬───────┘
  Meta, SMS, email           Redpanda         Postgres     Claude / Bedrock     APNs, FCM
  (via egress-proxy)    (requests, events)  (dedup, outbox)  (via egress-proxy)
```

- **One Spring Boot application, one image.** Each module is a package with its own
  settings, pools, metrics and `parity-gateway.modules.<name>.enabled` switch (all on by
  default).
- **Nothing in the call's media path.** Agents' audio goes to parity-ms directly; the
  gateway only issues TURN credentials.

### Stack (proposed)

| Part | Choice |
|---|---|
| Base | Java 21, Spring Boot 4.1, Maven, Lombok |
| HTTP and WebSockets | Spring Web MVC (webhooks, the API, server-sent events) and Spring WebSocket (agents, chat widget) |
| Security | Spring Security, OAuth2 resource server for Keycloak JWTs; signature checks per provider |
| Isolation | Resilience4j: circuit breaker, bulkhead and rate limiter per module and per connector |
| Kafka | Spring Kafka, with the parity message conventions |
| Store | PostgreSQL with JPA and Flyway: duplicate check, inbox and outbox, chat sessions |
| LLM | The Anthropic Java SDK, as in parity-customer-context |
| Push | APNs over HTTP/2 and FCM's HTTP v1 API (libraries chosen when the module is built) |
| Tests | Embedded Kafka and Postgres in Testcontainers, as the other services do |

### Modules (proposed)

**`api`: HTTPS in front of the Kafka APIs**
- `POST /api/v1/{service}/{cmd}` with the command's fields as the JSON body. The gateway
  adds `id` and `reply_to`, sends the request to the service's requests topic, and returns
  the answer.
- Which roles may send which command, for which tenant, is set per command. The tenant comes
  from the token, never from the body, except for platform admins.
- One reply topic, `gateway.replies`, read in full by every instance; each request's `id`
  is `<instance-id>:<uuid>`. Timeout 5 s, answered as 504 `not_confirmed` (a slow change can
  still land, as with parity-rs's retries).
- Errors: `bad_request` → 400, `not_found` → 404, `conflict` → 409, `internal` → 502.

**`channels`: the digital channels**
- The pipeline from §3.12, one connector at a time: **web chat**, then email, then WhatsApp,
  then the rest.
- Every message, in and out, is published to `channel.messages`, keyed by interaction id,
  with `interaction_id`, `tenant_id`, `conversation_id`, `customer_key`, `message_id`,
  `channel`, `direction`, `subject`, `text`, `language` and `at_ms`, so signal-detection
  reads it as it is.
- Duplicates are dropped by a unique key on the provider's message ID.
- Until interaction-engine exists there's no bot or agent to answer; a chat is only
  received, stored and published.

**`agents`: the agent desktop**
- `WS /agents`: the desktop connects with its JWT; any instance serves any agent.
- `GET /agents/turn`: time-limited TURN credentials (the TURN REST API's shared secret) and
  the ICE servers.
- **Now:** delivers customer-context's `consultation.requested` with `to: agent` to that
  agent, and forwards their `resolve` and `add_note`.
- **Later:** state, offers, accept and reject, call controls, message sending, the assist
  panel (with routing-engine, interaction-engine and agentanalytics).

**`llm`: every model call**
- `POST /llm/v1/messages` inside the zone, streamed as server-sent events when asked; Kafka
  for batch work.
- **First:** the provider chosen per tenant (Claude API to begin with), card numbers and
  IBANs removed from prompts, each call logged with its model and prompt version (no
  content), tokens counted per tenant.
- **Later:** residency routing to Bedrock or a self-hosted model, per-feature opt-in,
  versioned prompts, cost lines to billing, MCP tools from parity-context-mcp.
- No real API call without your go-ahead: it costs money, and no key is set.

**`push`: waking mobile softphones**
- `POST /push/v1/wake` from opensips when a call comes for a sleeping RFC 8599 contact
  (`pn-provider`, `pn-prid`, `pn-param`), sent on to APNs or FCM.
- Waits for the UCaaS decision (finding 12).

### Steps (proposed)

Each step is small. Stop after each for your details, as with the other services.

| # | Step | Done when |
|---|---|---|
| 0 | **Skeleton:** the app, module switches, healthy and drained files, JWT checks, metrics | It starts with any set of modules on, and drains cleanly |
| 1 | **`api`:** HTTPS → Kafka for parity-rs, parity-geoip and parity-customer-context, with roles per command | `ping` and `get_route` work over HTTPS; a slow answer comes back `not_confirmed` |
| 2 | **`channels`, web chat:** the widget's WebSocket, duplicate check, `channel.messages` | A chat message raises a signal in parity-signal-detection |
| 3 | **`agents`, first part:** sessions, TURN credentials, consultations for agents | An agent sees a consultation and resolves it |
| 4 | **`llm`, first part:** Claude through the gateway, redaction, logging, token counts | customer-context's consultations run through it |
| 5 | **`channels`, email** | An email becomes a `channel.messages` message, threaded |
| 6 | **`push`** | After the UCaaS decision |

### Startup and graceful shutdown

- `/tmp/parity-gateway.healthy` after startup; `/tmp/parity-gateway.drained` after a drain;
  both removed at startup.
- **On SIGTERM**, each enabled module drains at the same time:
  - `channels`: stop taking webhooks (providers retry, and duplicates are dropped), tell chat
    widgets to reconnect with 0–10 s of jitter, finish sends in progress;
  - `agents`: tell desktops to reconnect with 0–10 s of jitter; calls aren't affected, since
    media never passes through the gateway;
  - `api`: wait for the Kafka requests in flight to be answered or time out;
  - `llm`: let streamed calls finish; batch work is acknowledged only when done, so it's
    redone elsewhere;
  - `push`: finish the pushes in flight.
- Then the drained file. A 45 s drain timeout and a 60 s grace period (the docs give both
  gateways 60 s).

### Settings (proposed)

| Setting | Default |
|---|---|
| `parity-gateway.modules.{api,channels,agents,llm,push}.enabled` | `true` |
| `parity-gateway.api.replies-topic` / `request-timeout` | `gateway.replies` / `5s` |
| `parity-gateway.api.services.<name>.requests-topic` | `tenant.routing.requests`, `geoip.requests`, `customer.context.requests` |
| `parity-gateway.channels.messages-topic` | `channel.messages` |
| `parity-gateway.agents.turn.secret` / `ttl` / `uris` | none / `1h` / none |
| `parity-gateway.llm.provider` | `claude` (`none`) |
| `parity-gateway.lifecycle.healthy-file` / `drained-file` / `drain-timeout` | `/tmp/parity-gateway.healthy` / `.drained` / `45s` |
| `spring.security.oauth2.resourceserver.jwt.issuer-uri` | The Keycloak realm |

## Architecture docs to update (when you say so)

- The catalog, §2 and its map: channel-gateway, agent-gateway and the LLM gateway become
  modules of `parity-gateway`; push-gateway in `UCAAS.md` too.
- §3.8, §3.12, §3.18: the same; §3.18's agentanalytics no longer holds the LLM gateway.
- §7.3 (one drain row), §7.5 (one dashboard with a panel per module), §7.7 (egress), §8
  (its own Maven repo), §9.1 (agent-gateway in v1 is the `agents` module).
- §10: the merged-gateway risk (findings 3–5), and Kafka ACLs.

## Open questions

1. **The name and the shape:** is `parity-gateway` right? Is "one image, modules switched on
   per instance" what you meant by a single service, or should every instance always run all
   modules?
2. **The `api` module:** is HTTPS in front of the parity services' Kafka APIs what you had in
   mind? Who calls it: the admin console only, other services, or tenants' own systems too?
   Does the admin console then use it instead of its own Kafka client?
3. **The `llm` module:** should customer-context move its Claude calls to it? How much of
   §3.18 comes first: a plain pass-through, or redaction, residency and metering from the
   start?
4. **The `channels` module:** web chat first, as §9 says? And is publishing to
   `channel.messages` (feeding signal-detection) the right stand-in until interaction-engine
   exists?
5. **The `agents` module:** before routing-engine exists, is it right to start with sessions,
   TURN credentials and customer-context's consultations for agents?
6. **The `push` module:** wait for the UCaaS decision, or build it now against APNs and FCM?
7. **Production:** run every module together, or split customer-facing traffic, agents and
   the LLM keys into separate deployments of the same image (finding 3)?
8. **Where it runs:** per bin like channel-gateway, with shared provider connections and
   push in the live bin only (finding 6)?
9. **Kafka ACLs:** with the `api` module, or later (finding 10)?
10. **Drains:** coordinated between instances over Kafka like parity-rs, or only the two
    files?

## Logs (log-record.v1)

One JSON object per line on stdout, in parity-contracts' `log-record.v1` (CONVENTIONS "Logging"):
`ts`, `level`, `service`, `version`, `instance`, `incarnation`, `seq`, `event`, `msg`, the
correlation fields at the top level, the rest masked under `attrs`, `error` {code, kind, message,
stack at error}, `code` at warn and above, `repeat_count` for deduplicated events. Every event is
in `logging/events.parity-gateway.v1.json` (call sites: `log.atInfo().addKeyValue("event", "<name>")`;
`obs/LogCatalogueTest` keeps code and catalogue in step). Every record about a call (offers, reservations, wrap-ups, the call frames sent to a desktop, call commands, interaction events routed) is call-scoped: `xsid` (the interaction id, UUIDv7) and `tenant_id`; records about a desktop carry `agent_id` and `session_id`. Follow a call: `contract-check --root ../parity-contracts --logs <gateway.ndjson> --logs <ie.ndjson> --flow <xsid>`.
`scripts/ci.sh` checks every line the tests captured (`target/test-logs/`, `target/it-logs/`) with
`contract-check --catalogue logging/events.parity-gateway.v1.json --logs ...`; one violation fails the build.
