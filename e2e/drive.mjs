// parity-gateway step 5a: drives the gateway's own agent page in headless Chromium (Playwright's
// image, sandbox on, as pwuser), as an agent would: log in, Ready, accept an offer. It never
// answers a ringing call by hand: only the page's auto-answer rule may answer.
//   node drive.mjs <out dir> <page url> [max ms]
// Inputs (read-only mount /in): token (the agent's JWT, never logged), browser-880hz.wav (the
// fake microphone). Outputs in <out dir>: page.jsonl (the page's diagnostics log), stats.jsonl
// (getStats each second while a call is up), registered (marker), result.json. Stops when
// <out dir>/stop appears (the test writes it) or after max ms.
import fs from "node:fs";
import path from "node:path";
import { chromium } from "playwright";

const out = process.argv[2];
const url = process.argv[3];
const maxMs = Number(process.argv[4] ?? 600000);
const token = fs.readFileSync("/in/token", "utf8").trim();
const pageLog = fs.createWriteStream(path.join(out, "page.jsonl"));
const statsLog = fs.createWriteStream(path.join(out, "stats.jsonl"));
const note = (kind, data = {}) => pageLog.write(JSON.stringify({ t: Date.now(), kind, driver: true, ...data }) + "\n");
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const args = [
  "--allow-loopback-in-peer-connection", // parity-ms and freeswitch are on 127.0.0.1
  "--use-fake-ui-for-media-stream",
  "--use-fake-device-for-media-stream",
  "--use-file-for-fake-audio-capture=/in/browser-880hz.wav",
  "--autoplay-policy=no-user-gesture-required",
  "--host-resolver-rules=MAP acme.localhost 127.0.0.1",
];
const browser = await chromium.launch({ chromiumSandbox: true, args });
note("browser_launched", { version: browser.version() });
const context = await browser.newContext({ permissions: ["microphone"] });
const page = await context.newPage();
page.on("console", (m) => {
  if (m.type() === "error" || m.type() === "warning") note("console_" + m.type(), { text: m.text().slice(0, 300) });
});
page.on("pageerror", (e) => note("page_error", { error: String(e).slice(0, 300) }));

// In the page: what the agent hears, as Goertzel powers per 10 s window (no audio is kept).
function startCapture() {
  const stream = document.getElementById("remote").srcObject;
  const ctx = new AudioContext({ sampleRate: 48000 });
  const src = ctx.createMediaStreamSource(stream);
  const proc = ctx.createScriptProcessor(4096, 1, 1);
  const freqs = [440, 660, 880, 1000];
  const coef = freqs.map((f) => 2 * Math.cos((2 * Math.PI * f) / 48000));
  const win = 48000 * 10;
  const fresh = () => ({ n: 0, sq: 0, s1: freqs.map(() => 0), s2: freqs.map(() => 0) });
  const st = { started: Date.now(), windows: [], cur: fresh() };
  proc.onaudioprocess = (e) => {
    const x = e.inputBuffer.getChannelData(0);
    for (let i = 0; i < x.length; i++) {
      const w = st.cur;
      const v = x[i];
      w.sq += v * v;
      for (let k = 0; k < freqs.length; k++) {
        const s0 = v + coef[k] * w.s1[k] - w.s2[k];
        w.s2[k] = w.s1[k];
        w.s1[k] = s0;
      }
      if (++w.n === win) {
        const power = freqs.map((_, k) => w.s1[k] * w.s1[k] + w.s2[k] * w.s2[k] - coef[k] * w.s1[k] * w.s2[k]);
        const others = (power[1] + power[2] + power[3]) / 3;
        st.windows.push({ end_ms: Date.now(), rms: Math.sqrt(w.sq / win), ratio_440: power[0] / Math.max(1e-12, others) });
        st.cur = fresh();
      }
    }
  };
  src.connect(proc);
  proc.connect(ctx.destination);
  window.__capture = st;
}

async function stats() {
  return page.evaluate(async () => {
    const pc = window.parityAgent.peerConnection();
    if (!pc) return null;
    const r = await pc.getStats();
    const byId = new Map();
    r.forEach((s) => byId.set(s.id, s));
    const o = { t: Date.now(), connection: pc.connectionState };
    r.forEach((s) => {
      if (s.type === "inbound-rtp" && s.kind === "audio") {
        o.in = { packets: s.packetsReceived, lost: s.packetsLost, jitter: s.jitter, codec: byId.get(s.codecId)?.mimeType };
      }
      if (s.type === "outbound-rtp" && s.kind === "audio") {
        o.out = { packets: s.packetsSent, codec: byId.get(s.codecId)?.mimeType };
      }
      if (s.type === "transport" && s.selectedCandidatePairId) {
        const p = byId.get(s.selectedCandidatePairId);
        const rc = p && byId.get(p.remoteCandidateId);
        o.transport = { dtls: s.dtlsState, srtp: s.srtpCipher, remote: rc ? `${rc.address}:${rc.port}` : null };
      }
    });
    return o;
  });
}

let code = 0;
let seen = 0;
let capturing = false;
let lastStats = null;
const accepted = new Set();
const t0 = Date.now();
try {
  await page.goto(url);
  await page.fill("#token", token);
  await page.click("#connect");
  await page.waitForFunction(() => window.parityAgent?.log.some((e) => e.kind === "frame" && e.type === "snapshot"), null, { timeout: 15000 });
  await page.click('[data-state="ready"]');
  await page.waitForFunction(() => window.parityAgent.log.some((e) => e.kind === "sip_registration" && e.state === "Registered"), null, { timeout: 15000 });
  fs.writeFileSync(path.join(out, "registered"), String(Date.now()));
  let nextStats = 0;
  while (!fs.existsSync(path.join(out, "stop")) && Date.now() - t0 < maxMs) {
    const fresh = await page.evaluate((from) => window.parityAgent.log.slice(from), seen);
    for (const e of fresh) pageLog.write(JSON.stringify(e) + "\n");
    seen += fresh.length;
    if (await page.isVisible("#offer")) {
      const offerId = await page.getAttribute("#offer", "data-offer-id");
      if (offerId && !accepted.has(offerId)) {
        accepted.add(offerId);
        note("driver_accept", { offer_id: offerId });
        await page.click("#accept");
      }
    }
    if (!capturing && fresh.some((e) => e.kind === "answered")) {
      await page.waitForFunction(() => document.getElementById("remote").srcObject?.getAudioTracks().length > 0, null, { timeout: 5000 });
      await page.evaluate(startCapture);
      capturing = true;
      note("driver_capture_started");
    }
    if (Date.now() >= nextStats) {
      nextStats = Date.now() + 1000;
      const s = await stats();
      if (s) {
        statsLog.write(JSON.stringify(s) + "\n");
        lastStats = s;
      }
    }
    await sleep(250);
  }
  const fresh = await page.evaluate((from) => window.parityAgent.log.slice(from), seen);
  for (const e of fresh) pageLog.write(JSON.stringify(e) + "\n");
  const capture = await page.evaluate(() => (window.__capture ? { started: window.__capture.started, windows: window.__capture.windows } : null));
  fs.writeFileSync(path.join(out, "result.json"), JSON.stringify({ last_stats: lastStats, capture }, null, 1));
} catch (e) {
  note("driver_failed", { error: String(e).slice(0, 500) });
  code = 1;
} finally {
  await browser.close();
  pageLog.end();
  statsLog.end();
}
process.exit(code);
