package parity.gateway.e2e;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The PSTN side of step 5a: a minimal SIP UAC over UDP to freeswitch's carrier profile (5080),
 * with the headers voice-api's edge would set (X-Interaction-ID, X-Tenant-ID, X-CC-Bin,
 * X-CC-Trunk), and an RTP peer per call that sends a 440 Hz PCMU tone every 20 ms and measures
 * what comes back: every packet's arrival time and, per 10 s window, the 880 Hz (the browser's
 * tone) Goertzel ratio. It answers BYE, and counts any re-INVITE or UPDATE it gets.
 */
final class Carrier implements AutoCloseable {

    private static final InetAddress LOOPBACK = InetAddress.getLoopbackAddress();
    private static final InetSocketAddress FREESWITCH = new InetSocketAddress(LOOPBACK, 5080);
    private static final Pattern TAG = Pattern.compile(";tag=([^;>\\s]+)");
    private static final Pattern URI_IN_BRACKETS = Pattern.compile("<([^>]+)>");
    private static final byte[] TONE = tone(440);

    private final DatagramSocket sip;
    private final int sipPort;
    private final Map<String, Call> calls = new ConcurrentHashMap<>();
    private final java.util.Set<String> provisional = ConcurrentHashMap.newKeySet();
    private final ScheduledExecutorService timer =
            Executors.newScheduledThreadPool(2, Thread.ofPlatform().name("carrier-rtp-", 0).daemon(true).factory());
    private final Thread reader;
    private final List<String> ladder = Collections.synchronizedList(new ArrayList<>());
    private final long t0 = System.currentTimeMillis();

    Carrier(int sipPort) throws IOException {
        this.sipPort = sipPort;
        this.sip = new DatagramSocket(new InetSocketAddress(LOOPBACK, sipPort));
        this.reader = Thread.ofPlatform().name("carrier-sip").daemon(true).start(this::readLoop);
    }

    /** One call: its dialog and its media measurements. */
    static final class Call {
        final String callId;
        final String interactionId;
        final String fromTag = UUID.randomUUID().toString().substring(0, 8);
        final DatagramSocket rtp;
        final int rtpPort;
        volatile String toTag;
        volatile String remoteTarget;
        volatile InetSocketAddress media;
        volatile int finalStatus;
        volatile int byeStatus;
        volatile boolean ended;
        final java.util.concurrent.atomic.AtomicInteger reinvites = new java.util.concurrent.atomic.AtomicInteger();
        final java.util.concurrent.atomic.AtomicInteger updates = new java.util.concurrent.atomic.AtomicInteger();
        volatile long answeredAtMs;
        volatile long endedAtMs;
        final List<Long> arrivalsMs = Collections.synchronizedList(new ArrayList<>());
        final List<double[]> windows = Collections.synchronizedList(new ArrayList<>()); // {end ms, ratio 880, packets}
        final java.util.concurrent.atomic.AtomicLong sent = new java.util.concurrent.atomic.AtomicLong();
        volatile int payloadType = -1;
        ScheduledFuture<?> sender;
        Thread receiver;

        Call(String callId, String interactionId, int rtpPort) throws IOException {
            this.callId = callId;
            this.interactionId = interactionId;
            this.rtpPort = rtpPort;
            this.rtp = new DatagramSocket(new InetSocketAddress(LOOPBACK, rtpPort));
            this.rtp.setSoTimeout(200);
        }

        long received() {
            return arrivalsMs.size();
        }

        /** Packets that arrived in [fromMs, toMs). */
        long receivedBetween(long fromMs, long toMs) {
            synchronized (arrivalsMs) {
                return arrivalsMs.stream().filter(t -> t >= fromMs && t < toMs).count();
            }
        }

        /** The longest silence between two packets in [fromMs, toMs], ms. */
        long maxGapBetween(long fromMs, long toMs) {
            long gap = 0;
            long prev = fromMs;
            synchronized (arrivalsMs) {
                for (long t : arrivalsMs) {
                    if (t < fromMs || t > toMs) {
                        continue;
                    }
                    gap = Math.max(gap, t - prev);
                    prev = t;
                }
            }
            return Math.max(gap, toMs - prev);
        }
    }

    /** Sends the INVITE (retransmitted every 500 ms until an answer) and returns at once. */
    Call call(String interactionId, String number, int rtpPort) throws IOException {
        String callId = UUID.randomUUID() + "@gw-e2e";
        Call c = new Call(callId, interactionId, rtpPort);
        calls.put(callId, c);
        String sdp = sdp(rtpPort);
        String invite = "INVITE sip:" + number + "@127.0.0.1:5080 SIP/2.0\r\n"
                + via() + "Max-Forwards: 70\r\n"
                + "From: <sip:+447700900123@127.0.0.1:" + sipPort + ">;tag=" + c.fromTag + "\r\n"
                + "To: <sip:" + number + "@127.0.0.1:5080>\r\n"
                + "Call-ID: " + callId + "\r\nCSeq: 1 INVITE\r\n"
                + "Contact: <sip:carrier@127.0.0.1:" + sipPort + ">\r\n"
                + "X-Interaction-ID: " + interactionId + "\r\nX-Tenant-ID: acme\r\nX-CC-Bin: bin-a\r\nX-CC-Trunk: carrier-a\r\n"
                + "Content-Type: application/sdp\r\nContent-Length: " + sdp.length() + "\r\n\r\n" + sdp;
        Thread.ofVirtual().name("carrier-invite").start(() -> {
            try {
                for (int i = 0; i < 8 && c.finalStatus == 0 && !c.ended; i++) {
                    if (i == 0 || (c.toTag == null && !provisional.contains(callId))) {
                        send(invite);
                    }
                    Thread.sleep(500);
                }
            } catch (IOException e) {
                note("invite send failed");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        return c;
    }

    void bye(Call c) throws IOException {
        String bye = "BYE " + c.remoteTarget + " SIP/2.0\r\n" + via() + "Max-Forwards: 70\r\n"
                + "From: <sip:+447700900123@127.0.0.1:" + sipPort + ">;tag=" + c.fromTag + "\r\n"
                + "To: <sip:x@127.0.0.1:5080>;tag=" + c.toTag + "\r\n"
                + "Call-ID: " + c.callId + "\r\nCSeq: 2 BYE\r\nContent-Length: 0\r\n\r\n";
        send(bye);
        stopMedia(c);
    }

    List<String> ladder() {
        synchronized (ladder) {
            return List.copyOf(ladder);
        }
    }

    // ---- SIP ------------------------------------------------------------------------------

    private void readLoop() {
        byte[] buf = new byte[65_535];
        while (!sip.isClosed()) {
            DatagramPacket p = new DatagramPacket(buf, buf.length);
            try {
                sip.receive(p);
                onMessage(new String(p.getData(), 0, p.getLength(), StandardCharsets.UTF_8));
            } catch (IOException e) {
                if (!sip.isClosed()) {
                    note("sip read failed");
                }
            }
        }
    }

    private void onMessage(String m) throws IOException {
        int split = m.indexOf("\r\n\r\n");
        String head = split < 0 ? m : m.substring(0, split);
        String body = split < 0 ? "" : m.substring(split + 4);
        List<String> lines = head.lines().toList();
        String first = lines.get(0);
        String callId = header(lines, "Call-ID", "i");
        String cseq = header(lines, "CSeq", null);
        Call c = callId == null ? null : calls.get(callId);
        note("in  " + first + " (" + cseq + ")");
        if (first.startsWith("SIP/2.0 ")) {
            int status = Integer.parseInt(first.substring(8, 11));
            if (c == null || cseq == null) {
                return;
            }
            if (cseq.endsWith("INVITE")) {
                onInviteResponse(c, status, lines, body);
            } else if (cseq.endsWith("BYE")) {
                c.byeStatus = status;
                c.ended = true;
                c.endedAtMs = System.currentTimeMillis();
            }
            return;
        }
        String method = first.substring(0, first.indexOf(' '));
        if ("ACK".equals(method)) {
            return;
        }
        if (c != null && "BYE".equals(method)) {
            c.ended = true;
            c.endedAtMs = System.currentTimeMillis();
            stopMedia(c);
        } else if (c != null && "INVITE".equals(method)) {
            c.reinvites.incrementAndGet();
        } else if (c != null && "UPDATE".equals(method)) {
            c.updates.incrementAndGet();
        }
        String answer = "INVITE".equals(method) && c != null ? sdp(c.rtpPort) : "";
        StringBuilder r = new StringBuilder("SIP/2.0 200 OK\r\n");
        for (String l : lines) {
            String lower = l.toLowerCase(Locale.ROOT);
            if (lower.startsWith("via:") || lower.startsWith("v:") || lower.startsWith("from:") || lower.startsWith("f:")
                    || lower.startsWith("to:") || lower.startsWith("t:") || lower.startsWith("call-id:")
                    || lower.startsWith("i:") || lower.startsWith("cseq:")) {
                r.append(l).append("\r\n");
            }
        }
        if (!answer.isEmpty()) {
            r.append("Contact: <sip:carrier@127.0.0.1:").append(sipPort).append(">\r\nContent-Type: application/sdp\r\n");
        }
        r.append("Content-Length: ").append(answer.length()).append("\r\n\r\n").append(answer);
        send(r.toString());
    }

    private void onInviteResponse(Call c, int status, List<String> lines, String body) throws IOException {
        if (status < 200) {
            provisional.add(c.callId);
            return;
        }
        String to = header(lines, "To", "t");
        Matcher tag = TAG.matcher(to == null ? "" : to);
        if (tag.find()) {
            c.toTag = tag.group(1);
        }
        String contact = header(lines, "Contact", "m");
        Matcher uri = URI_IN_BRACKETS.matcher(contact == null ? "" : contact);
        if (uri.find()) {
            c.remoteTarget = uri.group(1);
        }
        String branch = "z9hG4bK-" + UUID.randomUUID();
        String ack = "ACK " + (status == 200 && c.remoteTarget != null ? c.remoteTarget : "sip:x@127.0.0.1:5080")
                + " SIP/2.0\r\nVia: SIP/2.0/UDP 127.0.0.1:" + sipPort + ";branch=" + branch + "\r\nMax-Forwards: 70\r\n"
                + "From: <sip:+447700900123@127.0.0.1:" + sipPort + ">;tag=" + c.fromTag + "\r\n"
                + "To: " + to + "\r\nCall-ID: " + c.callId + "\r\nCSeq: 1 ACK\r\nContent-Length: 0\r\n\r\n";
        send(ack);
        if (c.finalStatus != 0) {
            return; // a retransmitted final response: ACKed again, nothing else
        }
        c.finalStatus = status;
        if (status != 200) {
            c.ended = true;
            return;
        }
        c.answeredAtMs = System.currentTimeMillis();
        Matcher ip = Pattern.compile("(?m)^c=IN IP4 (\\S+)").matcher(body);
        Matcher port = Pattern.compile("(?m)^m=audio (\\d+)").matcher(body);
        if (ip.find() && port.find()) {
            c.media = new InetSocketAddress(InetAddress.getByName(ip.group(1)), Integer.parseInt(port.group(1)));
            startMedia(c);
        }
    }

    private static String header(List<String> lines, String name, String compact) {
        for (String l : lines) {
            int colon = l.indexOf(':');
            if (colon <= 0) {
                continue;
            }
            String n = l.substring(0, colon).trim();
            if (n.equalsIgnoreCase(name) || (compact != null && n.equalsIgnoreCase(compact))) {
                return l.substring(colon + 1).trim();
            }
        }
        return null;
    }

    private String via() {
        return "Via: SIP/2.0/UDP 127.0.0.1:" + sipPort + ";branch=z9hG4bK-" + UUID.randomUUID() + ";rport\r\n";
    }

    private static String sdp(int rtpPort) {
        return "v=0\r\no=carrier 1 1 IN IP4 127.0.0.1\r\ns=-\r\nc=IN IP4 127.0.0.1\r\nt=0 0\r\n"
                + "m=audio " + rtpPort + " RTP/AVP 0 101\r\na=rtpmap:0 PCMU/8000\r\na=rtpmap:101 telephone-event/8000\r\n"
                + "a=fmtp:101 0-16\r\na=ptime:20\r\na=sendrecv\r\n";
    }

    private void send(String m) throws IOException {
        byte[] b = m.getBytes(StandardCharsets.UTF_8);
        sip.send(new DatagramPacket(b, b.length, FREESWITCH));
        note("out " + m.substring(0, m.indexOf('\r')));
    }

    private void note(String s) {
        ladder.add(String.format(Locale.ROOT, "%8.3f %s", (System.currentTimeMillis() - t0) / 1000.0, s));
    }

    // ---- RTP ------------------------------------------------------------------------------

    private void startMedia(Call c) {
        int ssrc = (int) (Math.random() * Integer.MAX_VALUE);
        int[] seq = {0};
        c.sender = timer.scheduleAtFixedRate(() -> {
            byte[] p = new byte[12 + 160];
            int s = seq[0]++;
            long ts = 160L * s;
            p[0] = (byte) 0x80;
            p[1] = 0;
            p[2] = (byte) (s >> 8);
            p[3] = (byte) s;
            for (int i = 0; i < 4; i++) {
                p[4 + i] = (byte) (ts >> (24 - 8 * i));
                p[8 + i] = (byte) (ssrc >> (24 - 8 * i));
            }
            int from = (160 * s) % 8000;
            System.arraycopy(TONE, from, p, 12, 160);
            try {
                c.rtp.send(new DatagramPacket(p, p.length, c.media));
                c.sent.incrementAndGet();
            } catch (IOException e) {
                note("rtp send failed");
            }
        }, 0, 20, TimeUnit.MILLISECONDS);
        c.receiver = Thread.ofPlatform().name("carrier-rtp-in").daemon(true).start(() -> receive(c));
    }

    private static void stopMedia(Call c) {
        if (c.sender != null) {
            c.sender.cancel(false);
        }
        c.rtp.close();
    }

    /** Counts every packet; per 10 s of PCMU samples, the 880 Hz power over the mean at 440, 660 and 1000 Hz. */
    private static void receive(Call c) {
        byte[] buf = new byte[2048];
        double[] freqs = {880, 440, 660, 1000};
        double[] coef = new double[freqs.length];
        for (int k = 0; k < freqs.length; k++) {
            coef[k] = 2 * Math.cos(2 * Math.PI * freqs[k] / 8000);
        }
        double[] s1 = new double[freqs.length];
        double[] s2 = new double[freqs.length];
        int n = 0;
        long packets = 0;
        while (!c.rtp.isClosed()) {
            DatagramPacket p = new DatagramPacket(buf, buf.length);
            try {
                c.rtp.receive(p);
            } catch (SocketTimeoutException e) {
                continue;
            } catch (IOException e) {
                return;
            }
            int len = p.getLength();
            if (len < 12 || (buf[0] & 0xc0) != 0x80) {
                continue;
            }
            c.arrivalsMs.add(System.currentTimeMillis());
            packets++;
            c.payloadType = buf[1] & 0x7f;
            int off = 12 + 4 * (buf[0] & 0x0f);
            if ((buf[0] & 0x10) != 0 && len >= off + 4) {
                off += 4 + 4 * (((buf[off + 2] & 0xff) << 8) | (buf[off + 3] & 0xff));
            }
            if (c.payloadType != 0) {
                continue;
            }
            for (int i = off; i < len; i++) {
                double v = ulaw(buf[i]) / 32768.0;
                for (int k = 0; k < freqs.length; k++) {
                    double s0 = v + coef[k] * s1[k] - s2[k];
                    s2[k] = s1[k];
                    s1[k] = s0;
                }
                if (++n == 80_000) {
                    double[] power = new double[freqs.length];
                    for (int k = 0; k < freqs.length; k++) {
                        power[k] = s1[k] * s1[k] + s2[k] * s2[k] - coef[k] * s1[k] * s2[k];
                        s1[k] = 0;
                        s2[k] = 0;
                    }
                    double others = (power[1] + power[2] + power[3]) / 3;
                    c.windows.add(new double[] {System.currentTimeMillis(), power[0] / Math.max(1e-12, others), packets});
                    n = 0;
                    packets = 0;
                }
            }
        }
    }

    /** One second of a tone in G.711 mu-law at 8 kHz (a whole number of cycles, so it loops). */
    private static byte[] tone(int hz) {
        byte[] b = new byte[8000];
        for (int i = 0; i < b.length; i++) {
            b[i] = linearToUlaw((int) (10_000 * Math.sin(2 * Math.PI * hz * i / 8000.0)));
        }
        return b;
    }

    static byte linearToUlaw(int sample) {
        int sign = sample < 0 ? 0x80 : 0;
        int s = Math.min(32_635, Math.abs(sample)) + 0x84;
        int exponent = 7;
        for (int mask = 0x4000; (s & mask) == 0 && exponent > 0; mask >>= 1) {
            exponent--;
        }
        int mantissa = (s >> (exponent + 3)) & 0x0f;
        return (byte) ~(sign | (exponent << 4) | mantissa);
    }

    static int ulaw(byte b) {
        int u = ~b & 0xff;
        int t = ((u & 0x0f) << 3) + 0x84;
        t <<= (u & 0x70) >> 4;
        return (u & 0x80) != 0 ? 0x84 - t : t - 0x84;
    }

    @Override
    public void close() {
        calls.values().forEach(Carrier::stopMedia);
        timer.shutdownNow();
        sip.close();
        reader.interrupt();
    }
}
