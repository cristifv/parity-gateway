package parity.gateway.e2e;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.ECDSASigner;
import com.nimbusds.jose.jwk.Curve;
import com.nimbusds.jose.jwk.ECKey;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.gen.ECKeyGenerator;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.Deque;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import parity.contracts.routing.v1.Assigned;
import parity.contracts.telephony.v1.Arrived;
import parity.contracts.telephony.v1.LegState;
import parity.contracts.telephony.v1.LegUpdate;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Step 5a's evidence (PLAN step 5; one run, a few minutes): the gateway's own agent page in headless
 * Chromium, registered over WS to freeswitch's {@code agents} profile with dev-only SIP credentials
 * (profile {@code dev}, GW-003), on TC's real node (real freeswitch with TC's configuration, TC's
 * jar, parity-ms), with a stand-in for IE driving TC and the gateway's fake RE.
 *
 * <ol>
 *   <li>Negative: freeswitch rings the agent with no X-Parity-Interaction-Id, then with a forged one;
 *       then TC adds an agent leg for a call RE never reserved. None is auto-answered.
 *   <li>Positive: a carrier call is queued, offered, accepted in the page; RE assigns it; TC's leg
 *       carries the reserved interaction's id and is auto-answered; two-way G.711 for 135 s with no
 *       re-INVITE, measured at the customer (RTP), in the browser (getStats, what it hears) and on
 *       the wire (a loopback pcap); hang-up, wrap-up.
 * </ol>
 *
 * Run: {@code mvn -B -ntp -Dtest=NONE -Dsurefire.failIfNoSpecifiedTests=false -Dgw.evidence=true
 * -Dgw.parityMs=<media-server> -Dit.test=RealNodeBrowserIT verify}. Evidence in
 * {@code evidence/step5a/}. Local secrets in {@code ../secrets/local-dev/} (created when missing,
 * mode 600, never printed); the work directory ({@code -Dgw.e2e.work}) is deleted at the end.
 */
@EnabledIfSystemProperty(named = "gw.evidence", matches = "true")
class RealNodeBrowserIT {

    // TC's real-node images (its Freeswitch.java, ServerTarget.java, BrowserAgent.java), by digest.
    static final String FS_IMAGE =
            "docker.io/safarov/freeswitch:1.10.12@sha256:b31c743f4c911a19687c61e3214968f2a24f93f9d3d667cc26284192e158ffc6";
    static final String ETCD_IMAGE =
            "gcr.io/etcd-development/etcd@sha256:7c6c239825d00e3f6328a69caafd54be92063acf0c2ce78b8394699f52b75dc3";
    static final String PLAYWRIGHT_IMAGE =
            "mcr.microsoft.com/playwright:v1.64.0-noble@sha256:06a9939e57531807f8d5fd76ce44b53165ffb7d7501d87ab10e285c20b1e971f";
    // tcpdump only (IE's spike tools image, local; recorded by id in inputs.txt).
    static final String TOOLS_IMAGE = "ie-spike-tools:fc43";

    static final int ETCD_PORT = 23_791;
    static final int MS_PORT = 22_232;
    static final int MEDIA_LOW = 25_000;
    static final int MEDIA_HIGH = 25_999;
    static final int TC_GRPC = 19_710;
    static final int TC_MGMT = 19_711;
    static final int GW_PORT = 8090;
    static final int GW_MGMT = 8091;
    static final int RE_PORT = 9802;
    static final int CARRIER_SIP = 5062;
    static final String TENANT = "acme";
    static final long HOLD_MS = 135_000;

    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final SecureRandom RANDOM = new SecureRandom();

    private final Map<String, String> report = new LinkedHashMap<>();
    private final Deque<AutoCloseable> closers = new ArrayDeque<>();
    // The agent: acme's first in parity-contracts' shared test tenants (tenants/test-tenants.v1.json).
    private String agent;
    private String contact;
    private Path base;
    private Path work;
    private Path evidence;
    private Path browserOut;

    @Test
    void browserAgentOnTheRealNode() throws Exception {
        base = Path.of(System.getProperty("user.dir")).toAbsolutePath();
        Path tcDir = base.resolve("../parity-telephony-controller").normalize();
        Path secrets = Path.of(System.getProperty("gw.secrets", base.resolve("../secrets/local-dev").normalize().toString()));
        String msBin = System.getProperty("gw.parityMs");
        assertNotNull(msBin, "set -Dgw.parityMs to a media-server built from parity-ms requirement 41");
        Path tcJar = Path.of(System.getProperty("gw.tcJar", tcDir.resolve("target/parity-telephony-controller-0.1.0.jar").toString()));
        Path gwJar = Path.of(System.getProperty("gw.jar"));
        evidence = base.resolve("evidence/step5a");
        Files.createDirectories(evidence);
        Path contracts = Path.of(System.getProperty("parity.contracts.dir", base.resolve("../parity-contracts").toString()));
        agent = JSON.readTree(Files.readString(contracts.resolve("tenants/test-tenants.v1.json"))).path("test").path("agents")
                .path(TENANT).path(0).asString();
        contact = "sip:" + agent + "@" + TENANT + ".agents.localhost";
        preflight();
        work = System.getProperty("gw.e2e.work") == null ? Files.createTempDirectory("gw-e2e")
                : Files.createDirectory(Path.of(System.getProperty("gw.e2e.work")));
        try {
            run(tcDir, secrets, Path.of(msBin), tcJar, gwJar);
        } finally {
            while (!closers.isEmpty()) {
                try {
                    closers.pop().close();
                } catch (Exception e) {
                    System.err.println("cleanup: " + e.getClass().getSimpleName());
                }
            }
            writeReport();
            if (!Boolean.getBoolean("gw.e2e.keep")) {
                deleteTree(work);
            }
        }
    }

    private void run(Path tcDir, Path secrets, Path msBin, Path tcJar, Path gwJar) throws Exception {
        // ---- inputs: secrets (created once), TC's configuration and binaries snapshotted ----------
        Secrets s = secrets(secrets, agent);
        Path fsConf = work.resolve("fs-conf");
        copyTree(tcDir.resolve("freeswitch/conf"), fsConf);
        Path directory = Files.createDirectory(work.resolve("fs-directory"));
        writeSecret(directory.resolve("agents.xml"), directoryXml(agent, s.agentPassword()));
        Path fsOut = work.resolve("fs-out");
        for (Path d : List.of(fsOut, fsOut.resolve("json_cdr"), fsOut.resolve("log"))) {
            Files.createDirectories(d);
            Files.setPosixFilePermissions(d, PosixFilePermissions.fromString("rwxrwxrwx"));
        }
        Path ms = work.resolve("media-server");
        Files.copy(msBin, ms);
        Files.setPosixFilePermissions(ms, PosixFilePermissions.fromString("rwx------"));
        Path tc = work.resolve("tc.jar");
        Files.copy(tcJar, tc);
        Path gw = work.resolve("gw.jar");
        Files.copy(gwJar, gw);
        Path seccomp = work.resolve("chromium-seccomp.json");
        Files.copy(tcDir.resolve("experiments/compose/chromium-seccomp.json"), seccomp);
        Path driver = Files.createDirectory(work.resolve("driver"));
        for (String f : List.of("package.json", "package-lock.json", "drive.mjs")) {
            Files.copy(base.resolve("e2e").resolve(f), driver.resolve(f));
        }
        exec(driver, Duration.ofMinutes(3), "npm", "ci", "--ignore-scripts", "--no-audit", "--no-fund");
        Path in = Files.createDirectory(work.resolve("browser-in"));
        Files.setPosixFilePermissions(in, PosixFilePermissions.fromString("rwxr-xr-x"));
        Files.write(in.resolve("browser-880hz.wav"), wav(880, 10));
        String token = token(s.jwtKey(), agent);
        Files.writeString(in.resolve("token"), token, StandardCharsets.UTF_8);
        Files.setPosixFilePermissions(in.resolve("token"), PosixFilePermissions.fromString("rw-r--r--"));
        Files.writeString(work.resolve("jwks.json"), new JWKSet(s.jwtKey().toPublicJWK()).toString());
        browserOut = Files.createDirectory(work.resolve("browser-out"));
        Files.setPosixFilePermissions(browserOut, PosixFilePermissions.fromString("rwxrwxrwx"));
        inputs(ms, tc, gw, fsConf, driver);

        // ---- the node: etcd, freeswitch, a capture, parity-ms, TC ----------------------------------
        String me = exec(work, Duration.ofSeconds(5), "id", "-u").strip() + ":" + exec(work, Duration.ofSeconds(5), "id", "-g").strip();
        Docker.run(List.of("run", "-d", "--name", "gw-e2e-etcd", "--label", Docker.LABEL, "--network", "host",
                "--security-opt", "label=disable", "--cap-drop", "ALL", "--user", me, "--tmpfs", "/tmp:mode=1777", ETCD_IMAGE,
                "etcd", "--name", "e2e", "--data-dir", "/tmp/etcd", "--listen-client-urls", "http://127.0.0.1:" + ETCD_PORT,
                "--advertise-client-urls", "http://127.0.0.1:" + ETCD_PORT, "--listen-peer-urls", "http://127.0.0.1:23801",
                "--initial-advertise-peer-urls", "http://127.0.0.1:23801", "--initial-cluster", "e2e=http://127.0.0.1:23801"), false);
        closers.push(() -> Docker.rm("gw-e2e-etcd"));
        Etcd etcd = new Etcd(URI.create("http://127.0.0.1:" + ETCD_PORT));
        StandInIe.await(etcd::healthy, Duration.ofSeconds(30), "etcd never became healthy");

        Docker.run(List.of("run", "-d", "--name", "gw-e2e-freeswitch", "--label", Docker.LABEL, "--network", "host",
                "--security-opt", "label=disable", "--cap-drop", "ALL", "--user", me, "--tmpfs", "/tmp:mode=1777",
                "-v", fsConf + ":/etc/freeswitch:ro", "-v", directory + ":/etc/freeswitch/directory:ro",
                "-v", s.eslFile() + ":/run/secrets/freeswitch_esl_password:ro",
                "-v", fsOut.resolve("json_cdr") + ":/var/log/freeswitch/json_cdr", "-v", fsOut + ":/tc-out",
                "--entrypoint", "/usr/bin/freeswitch", FS_IMAGE, "-nonat", "-nf", "-nc", "-np", "-conf", "/etc/freeswitch",
                "-log", "/tc-out/log", "-db", "/tmp/db", "-run", "/tmp/run", "-temp", "/tmp", "-cache", "/tmp/cache",
                "-storage", "/tmp/storage", "-recordings", "/tmp/rec", "-sounds", "/tmp/sounds"), false);
        closers.push(() -> {
            copyIfExists(fsOut.resolve("log/freeswitch.log"), evidence.resolve("freeswitch.log"));
            Docker.rm("gw-e2e-freeswitch");
        });
        StandInIe.await(() -> listening(5066) && listening(8021), Duration.ofSeconds(60), "freeswitch never listened");

        // tcpdump writes into a mounted directory, in immediate mode: otherwise libpcap's last, partly
        // filled buffer block (the hang-up) is never written when tcpdump stops.
        Path pcapDir = Files.createDirectory(work.resolve("pcap"));
        Files.setPosixFilePermissions(pcapDir, PosixFilePermissions.fromString("rwxrwxrwx"));
        Path pcap = pcapDir.resolve("lo.pcap");
        Process capture = new ProcessBuilder("docker", "run", "--rm", "--name", "gw-e2e-pcap", "--label", Docker.LABEL,
                "--network", "host", "--security-opt", "label=disable", "--user", "0:0", "--cap-drop", "ALL",
                "--cap-add", "NET_RAW", "--cap-add", "NET_ADMIN", "-v", pcapDir + ":/pcap", TOOLS_IMAGE, "tcpdump", "-Z", "root",
                "-i", "lo", "--immediate-mode", "-U", "-s", "0", "-w", "/pcap/lo.pcap", "tcp port 5066 or udp port 5080 or udp portrange "
                        + MEDIA_LOW + "-" + MEDIA_HIGH + " or udp port 6010 or udp port 6012")
                .redirectOutput(work.resolve("tcpdump.stdout").toFile()).redirectError(work.resolve("tcpdump.stderr").toFile()).start();
        closers.push(() -> {
            Docker.run(List.of("stop", "gw-e2e-pcap"), true);
            capture.waitFor(20, TimeUnit.SECONDS);
        });
        StandInIe.await(() -> read(work.resolve("tcpdump.stderr")).contains("listening on"), Duration.ofSeconds(30),
                "tcpdump never started");

        Path msDir = work.resolve("ms");
        for (String d : List.of("rec", "prompts", "health")) {
            Files.createDirectories(msDir.resolve(d));
        }
        Process parityMs = new ProcessBuilder(ms.toString(), "--node-id", "media-1", "--control-listen", "127.0.0.1:" + MS_PORT,
                "--media-ip", "127.0.0.1", "--advertise-ip", "127.0.0.1", "--port-min", Integer.toString(MEDIA_LOW),
                "--port-max", Integer.toString(MEDIA_HIGH), "--sched-rtp", "other:0", "--sched-watchdog", "other:0",
                "--rtp-threads", "2", "--idle-timeout-secs", "600", "--recording-dir", msDir.resolve("rec").toString(),
                "--prompt-dir", msDir.resolve("prompts").toString(), "--health-dir", msDir.resolve("health").toString(),
                "--log-json").redirectErrorStream(true).redirectOutput(evidence.resolve("parity-ms.log").toFile()).start();
        closers.push(() -> {
            parityMs.destroyForcibly();
            parityMs.waitFor(10, TimeUnit.SECONDS);
        });
        StandInIe.await(() -> listening(MS_PORT), Duration.ofSeconds(15), "parity-ms never listened");

        Path tcState = Files.createDirectory(work.resolve("tc"));
        Files.createDirectories(tcState.resolve("lock"));
        Files.createDirectories(tcState.resolve("journal"));
        Process tcProcess = new ProcessBuilder("java", "-jar", tc.toString(), "--spring.profiles.active=test",
                "--server.port=" + TC_MGMT, "--parity.telephony-controller.grpc.port=" + TC_GRPC,
                "--parity.telephony-controller.grpc.test-peer-header=true",
                "--parity.telephony-controller.node-id=media-1", "--parity.telephony-controller.bins=bin-a",
                "--parity.telephony-controller.lock-dir=" + tcState.resolve("lock"),
                "--parity.telephony-controller.journal.dir=" + tcState.resolve("journal"),
                "--parity.telephony-controller.lifecycle.healthy-file=" + tcState.resolve("tc.healthy"),
                "--parity.telephony-controller.lifecycle.drained-file=" + tcState.resolve("tc.drained"),
                "--parity.telephony-controller.etcd.endpoint=http://127.0.0.1:" + ETCD_PORT,
                "--parity.telephony-controller.node.esl.password-file=" + s.eslFile(),
                "--parity.telephony-controller.node.cdr-dir=" + fsOut.resolve("json_cdr"),
                "--parity.telephony-controller.node.parity-ms.port=" + MS_PORT,
                "--parity.telephony-controller.readiness.endpoints.parity-ms=127.0.0.1:" + MS_PORT,
                // The dev registrar: agents of acme.agents.localhost registered with freeswitch itself.
                "--parity.telephony-controller.node.agent-registrar-domain=acme.agents.localhost")
                .redirectErrorStream(true).redirectOutput(evidence.resolve("tc.log").toFile()).start();
        closers.push(() -> stop(tcProcess));
        StandInIe.await(() -> httpStatus("http://127.0.0.1:" + TC_MGMT + "/readyz") == 200, Duration.ofSeconds(90),
                "TC never became ready (evidence/step5a/tc.log)");

        // ---- the gateway: its jar, profiles fake (RE and IE fakes) and dev (the SIP passwords) -------
        Path gwState = Files.createDirectory(work.resolve("gw"));
        ProcessBuilder gwBuilder = new ProcessBuilder("java", "-jar", gw.toString(), "--spring.profiles.active=fake,dev",
                "--server.port=" + GW_PORT, "--management.server.port=" + GW_MGMT, "--parity.gateway.instance=gw-e2e-1",
                "--parity.gateway.lifecycle.healthy-file=" + gwState.resolve("gw.healthy"),
                "--parity.gateway.lifecycle.drained-file=" + gwState.resolve("gw.drained"),
                "--parity.gateway.agents.auth.jwks-file=" + work.resolve("jwks.json"),
                "--parity.gateway.agents.auth.issuer=parity-gateway-dev",
                "--parity.gateway.agents.sip.key-file=" + gwState.resolve("unused-in-dev"),
                "--parity.gateway.agents.turn.urls=")
                .redirectErrorStream(true).redirectOutput(evidence.resolve("gateway.log").toFile());
        gwBuilder.environment().put("DEV_SIP_PASSWORDS_FILE", s.passwordsFile().toString());
        Process gateway = gwBuilder.start();
        closers.push(() -> stop(gateway));
        StandInIe.await(() -> Files.exists(gwState.resolve("gw.healthy")), Duration.ofSeconds(60),
                "the gateway never became healthy (evidence/step5a/gateway.log)");
        HttpResponse<String> page = HttpClient.newHttpClient().send(HttpRequest.newBuilder(
                URI.create("http://127.0.0.1:" + GW_PORT + "/agent/index.html")).build(), HttpResponse.BodyHandlers.ofString());
        String csp = page.headers().firstValue("content-security-policy").orElse("");
        Files.writeString(evidence.resolve("agent-page-headers.txt"), page.headers().map().entrySet().stream()
                .filter(e -> !e.getKey().equals("date")).map(e -> e.getKey() + ": " + String.join(", ", e.getValue()))
                .sorted().reduce("", (a, b) -> a + b + "\n"));

        StandInIe ie = new StandInIe(TC_GRPC, RE_PORT, etcd);
        closers.push(() -> Files.write(evidence.resolve("standin-ie.txt"), ie.timeline()));
        closers.push(ie);
        ie.start();
        Carrier carrier = new Carrier(CARRIER_SIP);
        closers.push(() -> Files.write(evidence.resolve("carrier-sip.txt"), carrier.ladder()));
        closers.push(carrier);

        // ---- the browser: logs in, Ready, registers -------------------------------------------------
        Docker.run(List.of("run", "-d", "--name", "gw-e2e-browser", "--label", Docker.LABEL, "--network", "host",
                "--user", "pwuser", "--cap-drop", "ALL", "--cap-add", "SYS_CHROOT", "--security-opt", "label=disable",
                "--security-opt", "seccomp=" + seccomp, "--shm-size", "512m", "-v", driver + ":/driver:ro",
                "-v", in + ":/in:ro", "-v", browserOut + ":/out", "--entrypoint", "node", PLAYWRIGHT_IMAGE,
                "/driver/drive.mjs", "/out", "http://acme.localhost:" + GW_PORT + "/agent/index.html", "900000"), false);
        closers.push(() -> {
            Files.writeString(evidence.resolve("browser-container.log"), Docker.run(List.of("logs", "gw-e2e-browser"), true));
            Docker.rm("gw-e2e-browser");
            for (String f : List.of("page.jsonl", "stats.jsonl", "result.json")) {
                copyIfExists(browserOut.resolve(f), evidence.resolve("browser-" + f));
            }
        });
        StandInIe.await(() -> Files.exists(browserOut.resolve("registered")) || containerExited("gw-e2e-browser"),
                Duration.ofSeconds(90), "the browser never registered");
        assertTrue(Files.exists(browserOut.resolve("registered")), "the browser exited before registering: " + tail(browserOut.resolve("page.jsonl")));
        report.put("AC1 CSP header connect-src", csp.replaceAll(".*connect-src ([^;]*);.*", "$1"));

        // ---- negatives: no header; a forged header; TC's leg for an unreserved interaction -----------
        String noHeader = UUID.randomUUID().toString();
        report.put("N2 ESL originate, no X-Parity header", Esl.bgapi(s.eslPassword(),
                "originate {origination_uuid=" + noHeader + ",originate_timeout=8}user/" + agent + "@acme.agents.localhost &park()"));
        StandInIe.await(() -> events(e -> "call_state".equals(kind(e)) && e.path("interaction_id").isNull()
                && "Terminated".equals(e.path("state").asString(""))).size() >= 1, Duration.ofSeconds(25),
                "the headerless INVITE never ended in the page");
        String forged = UUID.randomUUID().toString();
        report.put("N3 ESL originate, forged X-Parity-Interaction-Id", Esl.bgapi(s.eslPassword(),
                "originate {origination_uuid=" + UUID.randomUUID() + ",originate_timeout=8,sip_h_X-Parity-Interaction-Id="
                        + forged + "}user/" + agent + "@acme.agents.localhost &park()"));
        StandInIe.await(() -> !events(e -> "call_state".equals(kind(e)) && forged.equals(e.path("interaction_id").asString(""))
                && "Terminated".equals(e.path("state").asString(""))).isEmpty(), Duration.ofSeconds(25),
                "the forged INVITE never ended in the page");

        String unreserved = UUID.randomUUID().toString();
        Carrier.Call callB = carrier.call(unreserved, "+15550100100", 6012);
        Arrived arrB = ie.awaitArrival(unreserved, Duration.ofSeconds(10));
        ie.claim(unreserved);
        ie.answer(unreserved, arrB.getLegId());
        StandInIe.await(() -> callB.finalStatus == 200, Duration.ofSeconds(10), "call B: the carrier got no 200");
        String legB = ie.addAgentLeg(unreserved, contact, 8);
        boolean legBEnded = ie.awaitLeg(legB, LegState.LEG_STATE_ENDED, Duration.ofSeconds(40));
        LegUpdate endB = ie.lastLeg(legB);
        report.put("N1 TC agent leg, unreserved interaction: states", ie.legStates(legB).toString()
                + (endB == null ? "" : " cause " + endB.getCause() + " sip " + endB.getSipStatus()));
        ie.endSession(unreserved);
        StandInIe.await(() -> callB.ended, Duration.ofSeconds(10), "call B: the carrier got no BYE");
        ie.release(unreserved);

        // ---- the reserved call: queue, offer, accept, auto-answer, 135 s of audio ---------------------
        String iid = UUID.randomUUID().toString();
        Carrier.Call callA = carrier.call(iid, "+15550100100", 6010);
        Arrived arrA = ie.awaitArrival(iid, Duration.ofSeconds(10));
        ie.claim(iid);
        ie.answer(iid, arrA.getLegId());
        StandInIe.await(() -> callA.finalStatus == 200, Duration.ofSeconds(10), "call A: the carrier got no 200");
        ie.enqueue(iid, "support");
        Assigned assigned = ie.awaitAssigned(iid, Duration.ofSeconds(30));
        report.put("P1 RE Assigned contact (expected " + contact + ")", assigned.getAgentContact());
        assertEquals(contact, assigned.getAgentContact(), "RE's contact for the agent isn't the gateway's");
        String legA = ie.addAgentLeg(iid, assigned.getAgentContact(), 20);
        assertTrue(ie.awaitLeg(legA, LegState.LEG_STATE_ANSWERED, Duration.ofSeconds(20)),
                "the agent leg was never answered: " + ie.legStates(legA) + " " + tail(browserOut.resolve("page.jsonl")));
        long answeredAt = System.currentTimeMillis();
        ie.connected(iid, assigned.getReservationId());
        Thread.sleep(10_000);
        long tenS = System.currentTimeMillis();
        report.put("P1 customer: RTP packets received, answer+2 s .. +10 s", Long.toString(callA.receivedBetween(answeredAt + 2000, tenS)));
        Thread.sleep(50_000);
        Files.writeString(evidence.resolve("parity-ms-query-at-60s.json"), parityMsQuery());
        Thread.sleep(Math.max(0, answeredAt + HOLD_MS - System.currentTimeMillis()));
        long endAt = System.currentTimeMillis();
        List<LegState> statesA = ie.legStates(legA);
        String metrics = tcMetrics();
        Files.writeString(evidence.resolve("tc-metrics-at-end.txt"), metrics);
        Files.writeString(evidence.resolve("parity-ms-query-at-end.json"), parityMsQuery());

        carrier.bye(callA);
        StandInIe.await(() -> ie.legStates(arrA.getLegId()).contains(LegState.LEG_STATE_ENDED), Duration.ofSeconds(10),
                "TC never saw the customer hang up");
        ie.endSession(iid);
        boolean agentLegEnded = ie.awaitLeg(legA, LegState.LEG_STATE_ENDED, Duration.ofSeconds(10));
        ie.callEnded(iid, agent, assigned.getReservationId());
        StandInIe.await(() -> !events(e -> "frame".equals(kind(e)) && "wrap_up".equals(e.path("type").asString(""))).isEmpty(),
                Duration.ofSeconds(10), "the page never got its wrap_up");
        ie.release(iid);
        Files.writeString(browserOut.resolve("stop"), "stop");
        StandInIe.await(() -> containerExited("gw-e2e-browser"), Duration.ofSeconds(60), "the browser driver never stopped");
        Docker.run(List.of("stop", "gw-e2e-pcap"), true);
        capture.waitFor(20, TimeUnit.SECONDS);

        // ---- measurements and the verdict -------------------------------------------------------------
        List<JsonNode> log = events(e -> true);
        Pcap wire = Pcap.read(pcap, MEDIA_LOW, MEDIA_HIGH);
        Files.write(evidence.resolve("sip-on-the-wire.txt"), wire.sip.stream().map(m -> String.format(Locale.ROOT,
                "%8.3f %-4s %-20s %-48s call-id=%s cseq=%s x-parity-interaction-id=%s x-parity-leg-id=%s session-expires=%s %s",
                (m.atMs() - answeredAt) / 1000.0, m.transport(), m.direction(), m.firstLine(), shortId(m.callId()), m.cseq(),
                m.interactionIds(), m.legIds(), m.sessionExpires(), m.mediaLine() == null ? "" : m.mediaLine())).toList());

        // AC1: REGISTER challenged and accepted over WS.
        report.put("AC1 registration (page)", log.stream().filter(e -> "sip_registration".equals(kind(e)))
                .map(e -> e.path("state").asString("")).toList().toString());
        long registers = wire.sip.stream().filter(m -> m.transport().equals("ws") && m.firstLine().startsWith("REGISTER ")).count();
        report.put("AC1 REGISTER over WS / 200 to REGISTER", registers + " / " + wire.sip.stream()
                .filter(m -> m.transport().equals("ws") && m.firstLine().startsWith("SIP/2.0 200") && String.valueOf(m.cseq()).endsWith("REGISTER")).count());
        assertTrue(log.stream().anyMatch(e -> "sip_registration".equals(kind(e)) && "Registered".equals(e.path("state").asString(""))),
                "AC1: the page never registered");
        assertTrue(csp.contains("ws://127.0.0.1:5066"), "AC1: the CSP doesn't allow the SIP WebSocket server");

        // AC2: only the reserved call is auto-answered.
        List<Pcap.Sip> invitesToBrowser = wire.sip.stream()
                .filter(m -> m.direction().equals("freeswitch->browser") && m.firstLine().startsWith("INVITE ")).toList();
        report.put("AC2 INVITEs to the browser (X-Parity-Interaction-Id values)", invitesToBrowser.stream()
                .map(m -> m.interactionIds().toString()).toList().toString());
        report.put("AC2 page decisions", log.stream().filter(e -> List.of("invite", "held_for_reservation", "ringing", "auto_answer",
                "answered").contains(kind(e))).map(e -> kind(e) + "(" + label(e.path("interaction_id"), iid, unreserved, forged) + ")")
                .toList().toString());
        for (String other : List.of(unreserved, forged)) {
            assertTrue(log.stream().anyMatch(e -> "ringing".equals(kind(e)) && other.equals(e.path("interaction_id").asString(""))),
                    "AC2: an unmatched INVITE didn't ring");
            assertFalse(log.stream().anyMatch(e -> ("auto_answer".equals(kind(e)) || "answered".equals(kind(e)))
                    && other.equals(e.path("interaction_id").asString(""))), "AC2: an unmatched INVITE was answered");
        }
        assertTrue(log.stream().anyMatch(e -> "invite".equals(kind(e)) && e.path("headers").asInt(-1) == 0
                && log.stream().noneMatch(a -> "answered".equals(kind(a)) && a.path("interaction_id").isNull())),
                "AC2: the headerless INVITE was answered or never came");
        assertTrue(legBEnded && !ie.legStates(legB).contains(LegState.LEG_STATE_ANSWERED), "AC2: TC's unreserved leg was answered");
        assertTrue(log.stream().anyMatch(e -> "auto_answer".equals(kind(e)) && iid.equals(e.path("interaction_id").asString(""))),
                "AC2: the reserved call wasn't auto-answered");
        Pcap.Sip inviteA = invitesToBrowser.stream().filter(m -> m.interactionIds().contains(iid)).findFirst().orElse(null);
        assertNotNull(inviteA, "TC-038: no INVITE to the browser carried the reserved interaction's id");
        assertEquals(List.of(iid), inviteA.interactionIds(), "TC-038: X-Parity-Interaction-Id not exactly once with TC's value");
        assertEquals(List.of(legA), inviteA.legIds(), "TC-038: X-Parity-Leg-Id not exactly once with the leg id");
        report.put("TC-038 agent INVITE over WS: X-Parity-Interaction-Id / X-Parity-Leg-Id", inviteA.interactionIds()
                + " / " + inviteA.legIds() + " (expected [" + iid.substring(0, 8) + "...] / [" + legA.substring(0, 8) + "...])");
        report.put("AC2 reserved interaction / unreserved / forged", iid + " / " + unreserved + " / " + forged);

        // AC3: two-way G.711 past two minutes, no re-INVITE.
        long heldMs = endAt - answeredAt;
        report.put("AC3 agent leg ANSWERED for (s), states at the end", heldMs / 1000 + ", " + statesA);
        assertTrue(heldMs >= 130_000 && !statesA.contains(LegState.LEG_STATE_ENDED), "AC3: the agent leg ended before two minutes");
        long from = answeredAt + 2000;
        long expected = (endAt - from) / 20;
        long custIn = callA.receivedBetween(from, endAt);
        long custGap = callA.maxGapBetween(from, endAt);
        report.put("AC3 customer: RTP from parity-ms (answer+2 s..end) / expected / max gap ms / payload type",
                custIn + " / " + expected + " / " + custGap + " / " + callA.payloadType);
        List<String> ratios880 = callA.windows.stream().filter(w -> w[0] > from && w[0] <= endAt).map(w -> String.format(Locale.ROOT, "%.0f", w[1])).toList();
        report.put("AC3 customer hears the browser's 880 Hz: ratio per 10 s window", ratios880.toString());
        report.put("AC3 customer: re-INVITEs / UPDATEs received", callA.reinvites.get() + " / " + callA.updates.get());
        JsonNode result = JSON.readTree(Files.readString(browserOut.resolve("result.json")));
        List<JsonNode> stats = Files.readAllLines(browserOut.resolve("stats.jsonl")).stream().map(JSON::readTree).toList();
        JsonNode first = stats.stream().filter(x -> x.path("t").asLong() >= from && x.has("in")).findFirst().orElseThrow();
        JsonNode last = stats.stream().filter(x -> x.path("t").asLong() <= endAt && x.has("in")).reduce((a, b) -> b).orElseThrow();
        long browserIn = last.path("in").path("packets").asLong() - first.path("in").path("packets").asLong();
        long browserOutPk = last.path("out").path("packets").asLong() - first.path("out").path("packets").asLong();
        long statsSpan = last.path("t").asLong() - first.path("t").asLong();
        long flatSeconds = flat(stats, from, endAt);
        report.put("AC3 browser getStats over (s): packets in / out / expected each / codec in, out / lost / DTLS, SRTP",
                statsSpan / 1000 + ": " + browserIn + " / " + browserOutPk + " / " + statsSpan / 20 + " / "
                        + last.path("in").path("codec").asString("") + ", " + last.path("out").path("codec").asString("") + " / "
                        + last.path("in").path("lost").asLong() + " / " + last.path("transport").path("dtls").asString("") + ", "
                        + last.path("transport").path("srtp").asString(""));
        report.put("AC3 browser: 1 s samples with no new packet in or out", Long.toString(flatSeconds));
        List<String> ratios440 = new ArrayList<>();
        // Windows that ended before the hang-up (endAt); the first one starts with the answer.
        result.path("capture").path("windows").forEach(w -> {
            if (w.path("end_ms").asLong() <= endAt) {
                ratios440.add(String.format(Locale.ROOT, "%.0f", w.path("ratio_440").asDouble()));
            }
        });
        report.put("AC3 browser hears the customer's 440 Hz: ratio per 10 s window", ratios440.toString());
        long reinvitesInPage = log.stream().filter(e -> "reinvite".equals(kind(e))).count();
        String agentCallId = inviteA.callId();
        List<Pcap.Sip> agentDialog = wire.sip.stream().filter(m -> agentCallId.equals(m.callId())).toList();
        long invitesInDialog = agentDialog.stream().filter(m -> m.firstLine().startsWith("INVITE ")).count();
        long updatesInDialog = agentDialog.stream().filter(m -> m.firstLine().startsWith("UPDATE ")).count();
        report.put("AC3 agent dialog on the wire: INVITEs / UPDATEs / Session-Expires / page re-INVITEs",
                invitesInDialog + " / " + updatesInDialog + " / " + inviteA.sessionExpires() + " / " + reinvitesInPage);
        report.put("AC3 agent INVITE offer", String.valueOf(inviteA.mediaLine()));
        long byes = agentDialog.stream().filter(m -> m.firstLine().startsWith("BYE ")).count();
        report.put("AC3 capture: SIP messages / agent dialog / BYE in it (the hang-up)",
                wire.sip.size() + " / " + agentDialog.stream().map(m -> m.firstLine().split(" ")[0] + (m.firstLine().startsWith("SIP/")
                        ? " " + m.firstLine().split(" ")[1] : "")).toList() + " / " + byes);
        List<String> flows = new ArrayList<>();
        for (Pcap.Flow f : wire.flows.values()) {
            flows.add(String.format(Locale.ROOT, "%5d -> %5d  packets %6d  rtp %6d  pt %s  rtp in window %6d  max gap ms %5d",
                    f.srcPort, f.dstPort, f.packets, f.rtp, f.payloadTypes, f.rtpBetween(from, endAt), f.maxGapBetween(from, endAt)));
        }
        flows.sort(null);
        Files.write(evidence.resolve("media-flows-on-the-wire.txt"), flows);
        report.put("AC3 media flows on the wire (see media-flows-on-the-wire.txt)", Integer.toString(flows.size()));
        report.put("AC3 TC canary (freeswitch RTP sockets) at the end", metric(metrics, "telephony_controller_freeswitch_rtp_ports_open"));
        report.put("P1 hang-up: agent leg ended / wrap_up frame", agentLegEnded + " / true");

        assertTrue(custIn >= expected * 95 / 100 && custGap < 200, "AC3: the customer's audio from the browser has gaps");
        assertTrue(ratios880.stream().allMatch(r -> Double.parseDouble(r) > 10) && ratios880.size() >= 12,
                "AC3: the customer doesn't hear the browser's tone throughout");
        assertTrue(browserIn >= statsSpan / 20 * 95 / 100 && browserOutPk >= statsSpan / 20 * 95 / 100, "AC3: the browser's audio has gaps");
        assertTrue(last.path("in").path("codec").asString("").equals("audio/PCMU") && last.path("out").path("codec").asString("").equals("audio/PCMU"),
                "AC3: not G.711 both ways");
        assertTrue(ratios440.size() >= 12 && ratios440.stream().skip(1).allMatch(r -> Double.parseDouble(r) > 10),
                "AC3: the browser doesn't hear the customer's tone throughout");
        assertEquals(1, invitesInDialog, "AC3: a re-INVITE reached the agent");
        assertEquals(1, byes, "the capture misses the agent's hang-up (its tail was lost)");
        assertEquals(0, updatesInDialog, "AC3: an UPDATE reached the agent");
        assertEquals(0, reinvitesInPage, "AC3: the page saw a re-INVITE");
        assertEquals(0, callA.reinvites.get() + callA.updates.get(), "AC3: the customer got a re-INVITE or UPDATE");
        report.put("verdict", "PASS");
    }

    // ---- secrets ----------------------------------------------------------------------------------

    /** The local-dev secrets: created once (mode 600 in a 700 directory), never printed. */
    record Secrets(Path passwordsFile, String agentPassword, Path eslFile, String eslPassword, ECKey jwtKey) {}

    private static Secrets secrets(Path dir, String agent) throws Exception {
        if (!Files.isDirectory(dir)) {
            Files.createDirectories(dir, PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
        }
        Files.setPosixFilePermissions(dir, PosixFilePermissions.fromString("rwx------"));
        Path passwords = dir.resolve("sip-agents.properties");
        if (!Files.exists(passwords)) {
            writeSecret(passwords, "# parity-gateway dev profile (GW-003): <tenant>/<agent>=<password>; local only\n");
        }
        if (Files.readAllLines(passwords).stream().noneMatch(l -> l.startsWith(TENANT + "/" + agent + "="))) {
            Files.writeString(passwords, TENANT + "/" + agent + "=" + random(24) + "\n", StandardCharsets.UTF_8,
                    java.nio.file.StandardOpenOption.APPEND);
        }
        Path esl = dir.resolve("freeswitch-esl.password");
        if (!Files.exists(esl)) {
            writeSecret(esl, random(18));
        }
        Path jwk = dir.resolve("gateway-dev-jwt.jwk.json");
        if (!Files.exists(jwk)) {
            writeSecret(jwk, new ECKeyGenerator(Curve.P_256).keyID("gw-dev-1").generate().toJSONString());
        }
        String agentPassword = Files.readAllLines(passwords).stream().filter(l -> l.startsWith(TENANT + "/" + agent + "="))
                .map(l -> l.substring(l.indexOf('=') + 1).strip()).findFirst().orElseThrow();
        return new Secrets(passwords, agentPassword, esl, Files.readString(esl).strip(), ECKey.parse(Files.readString(jwk)));
    }

    private static void writeSecret(Path p, String content) throws IOException {
        Files.deleteIfExists(p);
        Files.createFile(p, PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
        Files.writeString(p, content, StandardCharsets.UTF_8);
    }

    private static String random(int bytes) {
        byte[] b = new byte[bytes];
        RANDOM.nextBytes(b);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(b);
    }

    @SuppressWarnings("JavaUtilDate") // nimbus takes java.util.Date
    private static String token(ECKey key, String agent) throws Exception {
        JWTClaimsSet c = new JWTClaimsSet.Builder().issuer("parity-gateway-dev").subject(agent)
                .claim("tenant_id", TENANT).claim("agent_id", agent).claim("roles", List.of("agent"))
                .claim("sid", UUID.randomUUID().toString()).expirationTime(new Date(System.currentTimeMillis() + 1_800_000)).build();
        SignedJWT jwt = new SignedJWT(new JWSHeader.Builder(JWSAlgorithm.ES256).keyID(key.getKeyID()).build(), c);
        jwt.sign(new ECDSASigner(key));
        return jwt.serialize();
    }

    /**
     * freeswitch's directory for the dev registrar: the agents profile authenticates REGISTERs in
     * domain 127.0.0.1 (its force-register-domain); TC dials {@code user/<agent>@acme.agents.localhost}
     * (its agent-registrar-domain), whose dial string finds that registration.
     */
    private static String directoryXml(String agent, String password) {
        return "<include>\n  <!-- parity-gateway step 5a, generated from secrets/local-dev: never committed. -->\n"
                + "  <domain name=\"127.0.0.1\">\n    <params><param name=\"jsonrpc-allowed-methods\" value=\"\"/></params>\n"
                + "    <groups><group name=\"agents\"><users>\n      <user id=\"" + agent + "\"><params><param name=\"password\" value=\""
                + password + "\"/></params></user>\n    </users></group></groups>\n  </domain>\n"
                + "  <domain name=\"acme.agents.localhost\">\n    <params>\n"
                + "      <param name=\"dial-string\" value=\"{^^:sip_invite_domain=${dialed_domain}:presence_id=${dialed_user}@${dialed_domain}}"
                + "${sofia_contact(*/${dialed_user}@127.0.0.1)}\"/>\n      <param name=\"jsonrpc-allowed-methods\" value=\"\"/>\n"
                + "    </params>\n    <groups><group name=\"agents\"><users><user id=\"" + agent + "\"/></users></group></groups>\n"
                + "  </domain>\n</include>\n";
    }

    // ---- helpers ----------------------------------------------------------------------------------

    private void preflight() throws Exception {
        assertEquals(List.of(), Docker.others(), "another real-node run is using freeswitch's ports (TC tests, IE demo)");
        for (int p : List.of(5066, 5080, 5090, 5092, 8021, ETCD_PORT, MS_PORT, TC_GRPC, TC_MGMT, GW_PORT, GW_MGMT, RE_PORT, 9803)) {
            assertFalse(listening(p), "port " + p + " is in use");
        }
    }

    private void inputs(Path ms, Path tc, Path gw, Path fsConf, Path driver) throws Exception {
        List<String> lines = new ArrayList<>();
        lines.add("freeswitch image: " + FS_IMAGE);
        lines.add("etcd image: " + ETCD_IMAGE);
        lines.add("playwright image: " + PLAYWRIGHT_IMAGE);
        lines.add("tools image (tcpdump): " + TOOLS_IMAGE + " "
                + Docker.run(List.of("image", "inspect", "-f", "{{.Id}}", TOOLS_IMAGE), true).strip());
        lines.add("parity-ms media-server sha256: " + sha256(ms));
        lines.add("TC jar sha256: " + sha256(tc));
        lines.add("gateway jar sha256: " + sha256(gw));
        lines.add("driver drive.mjs sha256: " + sha256(driver.resolve("drive.mjs")));
        lines.add("freeswitch/conf (TC's, unchanged; directory/ replaced by the generated dev directory):");
        try (var files = Files.walk(fsConf)) {
            for (Path f : files.filter(Files::isRegularFile).sorted().toList()) {
                lines.add("  " + sha256(f) + "  " + fsConf.relativize(f));
            }
        }
        Files.write(evidence.resolve("inputs.txt"), lines);
    }

    private List<JsonNode> events(Predicate<JsonNode> keep) {
        Path f = browserOut.resolve("page.jsonl");
        List<JsonNode> out = new ArrayList<>();
        for (String line : read(f).lines().toList()) {
            JsonNode n = parse(line);
            if (n != null && keep.test(n)) {
                out.add(n);
            }
        }
        return out;
    }

    /** A JSON line, or null for one still being written. */
    private static JsonNode parse(String line) {
        try {
            return JSON.readTree(line);
        } catch (RuntimeException e) {
            return null;
        }
    }

    private static String kind(JsonNode e) {
        return e.path("kind").asString("");
    }

    private static String label(JsonNode id, String reserved, String unreserved, String forged) {
        String v = id.isNull() || id.isMissingNode() ? "" : id.asString("");
        if (v.isEmpty()) {
            return "none";
        }
        return v.equals(reserved) ? "reserved" : v.equals(unreserved) ? "unreserved" : v.equals(forged) ? "forged" : "other";
    }

    /** 1 s stats samples in [from, to] where neither direction's packet count grew. */
    private static long flat(List<JsonNode> stats, long from, long to) {
        long n = 0;
        JsonNode prev = null;
        for (JsonNode x : stats) {
            if (x.path("t").asLong() < from || x.path("t").asLong() > to || !x.has("in")) {
                continue;
            }
            if (prev != null && (x.path("in").path("packets").asLong() == prev.path("in").path("packets").asLong()
                    || x.path("out").path("packets").asLong() == prev.path("out").path("packets").asLong())) {
                n++;
            }
            prev = x;
        }
        return n;
    }

    private static String metric(String metrics, String name) {
        return metrics.lines().filter(l -> l.startsWith(name)).findFirst().orElse(name + " (absent)");
    }

    private static String tcMetrics() throws Exception {
        HttpResponse<String> r = HttpClient.newHttpClient().send(HttpRequest.newBuilder(
                URI.create("http://127.0.0.1:" + TC_MGMT + "/metrics")).build(), HttpResponse.BodyHandlers.ofString());
        return r.body().lines().filter(l -> l.startsWith("telephony_controller_") && !l.startsWith("#")).reduce("", (a, b) -> a + b + "\n");
    }

    /** parity-ms's own view of its calls: {@code list}, then {@code query} for each. */
    private static String parityMsQuery() throws IOException {
        StringBuilder b = new StringBuilder();
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), MS_PORT), 1000);
            s.setSoTimeout(3000);
            var in = new java.io.BufferedReader(new java.io.InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
            s.getOutputStream().write("{\"cmd\":\"list\",\"id\":\"l\"}\n".getBytes(StandardCharsets.UTF_8));
            String list = in.readLine();
            b.append(list).append('\n');
            for (JsonNode c : JSON.readTree(list).path("result").path("calls")) {
                String id = c.isString() ? c.asString() : c.path("call_id").asString("");
                s.getOutputStream().write(("{\"cmd\":\"query\",\"id\":\"q\",\"call_id\":\"" + id + "\"}\n").getBytes(StandardCharsets.UTF_8));
                b.append(in.readLine()).append('\n');
            }
        }
        return b.toString();
    }

    private static boolean listening(int port) {
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), port), 200);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    private static int httpStatus(String url) {
        try {
            return HttpClient.newHttpClient().send(HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(2)).build(),
                    HttpResponse.BodyHandlers.discarding()).statusCode();
        } catch (IOException e) {
            return -1;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return -1;
        }
    }

    private static boolean containerExited(String name) {
        try {
            return Docker.run(List.of("inspect", "-f", "{{.State.Running}}", name), true).strip().equals("false");
        } catch (IOException e) {
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private static String exec(Path dir, Duration within, String... cmd) throws Exception {
        Process p = new ProcessBuilder(cmd).directory(dir.toFile()).redirectErrorStream(true).start();
        String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        if (!p.waitFor(within.toMillis(), TimeUnit.MILLISECONDS) || p.exitValue() != 0) {
            throw new IOException(cmd[0] + " failed: " + out.lines().limit(10).toList());
        }
        return out;
    }

    private static void stop(Process p) throws InterruptedException {
        p.destroy();
        if (!p.waitFor(30, TimeUnit.SECONDS)) {
            p.destroyForcibly();
            p.waitFor(5, TimeUnit.SECONDS);
        }
    }

    private static String read(Path f) {
        try {
            return Files.exists(f) ? Files.readString(f) : "";
        } catch (IOException e) {
            return "";
        }
    }

    private static String tail(Path f) {
        List<String> l = read(f).lines().toList();
        return String.join("\n", l.subList(Math.max(0, l.size() - 15), l.size()));
    }

    private static String shortId(String id) {
        return id == null ? "-" : id.substring(0, Math.min(12, id.length()));
    }

    private static void copyIfExists(Path from, Path to) throws IOException {
        if (Files.exists(from)) {
            Files.copy(from, to, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private static String sha256(Path f) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(f)));
    }

    /** A tone as a 48 kHz mono 16-bit WAV (Chromium's fake microphone loops it). */
    private static byte[] wav(int hz, int seconds) {
        int n = 48_000 * seconds;
        ByteBuffer b = ByteBuffer.allocate(44 + 2 * n).order(ByteOrder.LITTLE_ENDIAN);
        b.put("RIFF".getBytes(StandardCharsets.US_ASCII)).putInt(36 + 2 * n).put("WAVEfmt ".getBytes(StandardCharsets.US_ASCII))
                .putInt(16).putShort((short) 1).putShort((short) 1).putInt(48_000).putInt(96_000).putShort((short) 2)
                .putShort((short) 16).put("data".getBytes(StandardCharsets.US_ASCII)).putInt(2 * n);
        for (int i = 0; i < n; i++) {
            b.putShort((short) (8000 * Math.sin(2 * Math.PI * hz * i / 48_000.0)));
        }
        return b.array();
    }

    private static void copyTree(Path from, Path to) throws IOException {
        try (var files = Files.walk(from)) {
            for (Path f : files.toList()) {
                Path t = to.resolve(from.relativize(f).toString());
                if (Files.isDirectory(f)) {
                    Files.createDirectories(t);
                } else {
                    Files.copy(f, t);
                }
            }
        }
    }

    private static void deleteTree(Path dir) throws IOException {
        if (!Files.exists(dir)) {
            return;
        }
        Files.walkFileTree(dir, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path d, IOException e) throws IOException {
                Files.delete(d);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    /** report.md, and no secret in any evidence file (checked without printing one). */
    private void writeReport() throws Exception {
        StringBuilder b = new StringBuilder("# parity-gateway step 5a: the browser agent on TC's real node (one run)\n\n")
                .append("Generated by RealNodeBrowserIT. Times are wall-clock; AC numbers refer to PLAN step 5 / the 5a brief.\n\n")
                .append("| Item | Measured |\n|---|---|\n");
        report.forEach((k, v) -> b.append("| ").append(k).append(" | ").append(v.replace("|", "\\|").replace("\n", " ")).append(" |\n"));
        if (!report.containsKey("verdict")) {
            b.append("| verdict | FAIL (see the test's output) |\n");
        }
        Files.writeString(evidence.resolve("report.md"), b.toString());
        System.out.print(b);
        Path secrets = Path.of(System.getProperty("gw.secrets", base.resolve("../secrets/local-dev").normalize().toString()));
        List<String> values = new ArrayList<>();
        if (Files.exists(secrets.resolve("sip-agents.properties"))) {
            Files.readAllLines(secrets.resolve("sip-agents.properties")).stream().filter(l -> l.contains("="))
                    .map(l -> l.substring(l.indexOf('=') + 1).strip()).forEach(values::add);
        }
        if (Files.exists(secrets.resolve("freeswitch-esl.password"))) {
            values.add(Files.readString(secrets.resolve("freeswitch-esl.password")).strip());
        }
        Path token = work.resolve("browser-in/token");
        if (Files.exists(token)) {
            values.add(Files.readString(token).strip());
        }
        try (var files = Files.list(evidence)) {
            for (Path f : files.filter(Files::isRegularFile).toList()) {
                String text = new String(Files.readAllBytes(f), StandardCharsets.ISO_8859_1);
                for (String v : values) {
                    if (!v.isEmpty() && text.contains(v)) {
                        Files.delete(f);
                        throw new AssertionError("a secret reached evidence file " + f.getFileName() + " (deleted)");
                    }
                }
            }
        }
    }
}
