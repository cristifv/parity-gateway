// The minimal agent page: agent-ws.v1 (parity-contracts schemas/agent-ws/) and the softphone rules
// (parity-gateway PLAN.md): auto-answer only an interaction RE reserved for this session; an
// INVITE that arrives first waits up to 2 s for its `reserved` frame, then rings for a manual
// answer (GW-008); never hold or re-INVITE over SIP (hold and mute go through IE); G.711 first.
// The CSP comes as a header from the gateway (connect-src names the SIP WebSocket servers).
const $ = (id) => document.getElementById(id);
const uuid = () => crypto.randomUUID();
let ws, token, backoff = 500, reserved = new Set(), call = null, wrap = null, offer = null, ua = null;
let iceServers = [], ringing = null;
const waiting = new Map(); // interaction id -> pending INVITE (GW-008)

// Diagnostics: a bounded log of what the page decided (no tokens, credentials, SDP or numbers),
// for support and the step-5 driver (window.parityAgent).
const diag = [];
function note(kind, data = {}) {
  diag.push({ t: Date.now(), kind, ...data });
  if (diag.length > 500) diag.shift();
}
window.parityAgent = {
  log: diag,
  peerConnection: () => ua?.session?.sessionDescriptionHandler?.peerConnection ?? null,
  sendDtmf: (tones) => sendDtmf(tones),
};

function send(type, fields = {}) {
  if (ws && ws.readyState === 1) ws.send(JSON.stringify({ v: 1, type, id: uuid(), ...fields }));
}
function show(id, on) { $(id).hidden = !on; }

function connect() {
  // Always wss, except an http page on a loopback host (the dev profile): a token never crosses a network in clear.
  const loopback = location.hostname === "localhost" || location.hostname === "127.0.0.1" || location.hostname.endsWith(".localhost");
  const scheme = location.protocol === "http:" && loopback ? "ws" : "wss";
  ws = new WebSocket(`${scheme}://${location.host}/agents/v1/ws`, "parity.agent.v1");
  ws.onopen = () => { note("ws_open"); ws.send(JSON.stringify({ v: 1, type: "auth", token })); };
  ws.onmessage = (e) => onFrame(JSON.parse(e.data));
  ws.onclose = (e) => {
    note("ws_closed", { code: e.code });
    if (e.code === 4001 || e.code === 4003 || e.code === 4009) { $("banner").textContent = `closed ${e.code}`; return; }
    const retry = /retry_after_ms=(\d+)/.exec(e.reason || "");
    const wait = retry ? Number(retry[1]) : Math.random() * backoff;
    backoff = Math.min(30000, backoff * 2);
    setTimeout(connect, wait);
  };
}

function onFrame(f) {
  if (f.type !== "ping") note("frame", { type: f.type, interaction_id: f.interaction_id, state: f.agent?.state });
  switch (f.type) {
    case "snapshot":
      backoff = 500;
      reserved = new Set(f.reservations.map((r) => r.interaction_id));
      setAgent(f.agent); offer = f.offer || null; showOffer();
      call = f.calls.find((c) => c.state !== "ended") || null; showCall();
      break;
    case "agent_state": setAgent(f.agent); break;
    case "offer": offer = f; showOffer(); break;
    case "offer_revoked": if (offer && offer.offer_id === f.offer_id) { offer = null; showOffer(); } break;
    case "reserved":
      reserved.add(f.interaction_id);
      if (offer && offer.interaction_id === f.interaction_id) { offer = null; showOffer(); }
      if (waiting.has(f.interaction_id)) {
        note("auto_answer", { interaction_id: f.interaction_id, late: true });
        answer(waiting.get(f.interaction_id)); waiting.delete(f.interaction_id);
      }
      break;
    case "call": call = f.call.state === "ended" ? null : f.call; showCall(); break;
    case "wrap_up": wrap = f; show("wrap", true); break;
    case "result": if (!f.ok) onError(f.error); break;
    case "ice_servers": iceServers = f.ice_servers; break;
    case "sip_credentials": startSoftphone(f); break;
    case "reconnect": ws.onclose = null; ws.close(1000); setTimeout(connect, f.after_ms); break;
    case "ping": ws.send(JSON.stringify({ v: 1, type: "pong" })); break;
    default: break;
  }
}

function onError(err) {
  $("banner").textContent = err.code;
  // IE refused or is unreachable: the call stays up; mute locally instead (shown as such).
  if ((err.code === "call_unmanaged" || err.code === "unavailable") && ua && ua.session) localMute(true);
}
function setAgent(a) { $("state").textContent = a.state + (a.mode === "campaign" ? " (campaign)" : "") + (a.pending_target ? ` -> ${a.pending_target}` : ""); }
function showOffer() {
  show("offer", !!offer);
  $("offer").dataset.offerId = offer ? offer.offer_id : "";
  if (offer) $("offer-info").textContent = `${offer.queue_id || "direct"}`;
}
function showCall() {
  show("call", !!call);
  if (call) $("call-info").textContent = `${call.held ? "held " : ""}${call.muted ? "muted " : ""}${call.managed ? "" : "(controls unavailable; the call continues)"}`;
}

// ---- softphone (SIP.js 0.21.2, vendored by scripts/vendor-sipjs.sh) ------------------------
async function startSoftphone(creds) {
  if (ua) return; // the token is renewed with the next registration (GW-014)
  const { UserAgent, Registerer } = await import("./vendor/sip.js/lib/index.js");
  ua = new UserAgent({
    uri: UserAgent.makeURI(creds.uri),
    transportOptions: { server: creds.ws_servers[0] },
    // The digest username is the URI's user part; the token is the password (the JWT's carriage: GW-014).
    authorizationUsername: creds.uri.slice(4).split("@")[0], authorizationPassword: creds.token,
    logLevel: "warn",
    sessionDescriptionHandlerFactoryOptions: {
      iceGatheringTimeout: 500,
      peerConnectionConfiguration: { iceServers: iceServers.map((s) => ({ urls: s.urls, username: s.username, credential: s.credential })) },
    },
    delegate: { onInvite: (inv) => onInvite(inv) },
  });
  await ua.start();
  const registerer = new Registerer(ua);
  registerer.stateChange.addListener((state) => note("sip_registration", { state }));
  await registerer.register();
}

function onInvite(inv) {
  const ids = inv.request.getHeaders("X-Parity-Interaction-Id");
  const id = ids.length === 1 ? ids[0].trim() : null; // none, or a repeat: never a match
  note("invite", { interaction_id: id, headers: ids.length, leg_id: inv.request.getHeader("X-Parity-Leg-Id") || null,
    session_expires: inv.request.getHeader("Session-Expires") || null });
  inv.stateChange.addListener((state) => {
    note("call_state", { interaction_id: id, state });
    if (state === "Terminated") {
      if (waiting.get(id) === inv) waiting.delete(id);
      if (ringing === inv) { ringing = null; show("ringing", false); }
      if (ua.session === inv) { ua.session = null; show("keypad", false); }
    }
  });
  // SIP.js answers an in-dialog re-INVITE itself; none should ever come (TC's rule): count them.
  inv.delegate = { onInvite: (_req, _res, status) => note("reinvite", { interaction_id: id, status }) };
  if (id && reserved.has(id)) { note("auto_answer", { interaction_id: id }); return answer(inv); }
  if (id) {
    note("held_for_reservation", { interaction_id: id });
    waiting.set(id, inv);
    setTimeout(() => { if (waiting.get(id) === inv) { waiting.delete(id); ring(inv); } }, 2000);
    return;
  }
  ring(inv); // never auto-answered without a match
}
function ring(inv) {
  note("ringing", { interaction_id: inv.request.getHeader("X-Parity-Interaction-Id") || null });
  ringing = inv;
  show("ringing", true);
  $("answer").onclick = () => { show("ringing", false); ringing = null; answer(inv); };
}
function answer(inv) {
  ua.session = inv;
  inv.accept({ sessionDescriptionHandlerOptions: { constraints: { audio: true, video: false } } }).then(() => {
    note("answered", { interaction_id: inv.request.getHeader("X-Parity-Interaction-Id") || null });
    show("keypad", true);
    const pc = inv.sessionDescriptionHandler.peerConnection;
    pc.getTransceivers().forEach((t) => {
      const codecs = RTCRtpReceiver.getCapabilities("audio").codecs;
      t.setCodecPreferences([...codecs.filter((c) => /PCM[UA]/.test(c.mimeType)), ...codecs.filter((c) => !/PCM[UA]/.test(c.mimeType))]);
    });
    const stream = new MediaStream();
    pc.getReceivers().forEach((r) => r.track && stream.addTrack(r.track));
    $("remote").srcObject = stream;
  }).catch((e) => note("answer_failed", { error: String(e).slice(0, 200) }));
}
// The agent's DTMF action codes (P5: `*1` pause and `*2` resume the recording, `*3` mute) go as
// RFC 4733 on the call; parity-ms matches them. The log counts tones, never says which.
function sendDtmf(tones) {
  const sdh = ua?.session?.sessionDescriptionHandler;
  const ok = !!sdh && /^[0-9*#A-D]{1,8}$/.test(tones) && sdh.sendDtmf(tones, { duration: 160, interToneGap: 120 });
  note("dtmf_sent", { tones: tones.length, ok: !!ok });
  return !!ok;
}
function localMute(on) {
  const pc = ua.session.sessionDescriptionHandler.peerConnection;
  pc.getSenders().forEach((s) => { if (s.track) s.track.enabled = !on; });
  $("banner").textContent = on ? "muted locally" : "";
}

// ---- controls ---------------------------------------------------------------------------
$("connect").onclick = () => { token = $("token").value; connect(); };
document.querySelectorAll("[data-state]").forEach((b) => (b.onclick = () => send("set_state", { state: b.dataset.state })));
$("accept").onclick = () => offer && send("accept", { offer_id: offer.offer_id });
$("reject").onclick = () => offer && send("reject", { offer_id: offer.offer_id });
document.querySelectorAll("[data-c]").forEach((b) => (b.onclick = () => call && send(b.dataset.c, { interaction_id: call.interaction_id })));
$("transfer").onclick = () => call && send("transfer", { interaction_id: call.interaction_id, target: { queue_id: $("queue").value } });
$("send-dtmf").onclick = () => { sendDtmf($("dtmf").value); $("dtmf").value = ""; };
$("done").onclick = () => { if (wrap) { send("wrap_up_done", { reservation_id: wrap.reservation_id, disposition_code: $("disposition").value || "none" }); show("wrap", false); wrap = null; } };
