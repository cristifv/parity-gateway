package parity.gateway.e2e;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeSet;

/**
 * Reads the loopback capture of step 5a: SIP over WebSocket on TCP 5066 (the agent's softphone;
 * frames unmasked), SIP over UDP 5080 (the carrier), and the UDP media flows (RTP, SRTP, DTLS,
 * STUN) to and from parity-ms's ports. IPv4 only, Ethernet or Linux cooked framing.
 */
final class Pcap {

    /** One SIP message as seen on the wire. */
    record Sip(long atMs, String transport, String direction, String firstLine, String callId, String cseq,
            List<String> interactionIds, List<String> legIds, String sessionExpires, String mediaLine) {}

    /** One UDP flow (one direction). */
    static final class Flow {
        final int srcPort;
        final int dstPort;
        long packets;
        long rtp;
        final TreeSet<Integer> payloadTypes = new TreeSet<>();
        final List<Long> rtpAtMs = new ArrayList<>();

        Flow(int srcPort, int dstPort) {
            this.srcPort = srcPort;
            this.dstPort = dstPort;
        }

        long rtpBetween(long fromMs, long toMs) {
            return rtpAtMs.stream().filter(t -> t >= fromMs && t < toMs).count();
        }

        long maxGapBetween(long fromMs, long toMs) {
            long gap = 0;
            long prev = fromMs;
            for (long t : rtpAtMs) {
                if (t < fromMs || t > toMs) {
                    continue;
                }
                gap = Math.max(gap, t - prev);
                prev = t;
            }
            return Math.max(gap, toMs - prev);
        }
    }

    final List<Sip> sip = new ArrayList<>();
    final Map<String, Flow> flows = new HashMap<>();
    private final Map<String, ByteArrayOutputStream> streams = new HashMap<>();
    private final int mediaLow;
    private final int mediaHigh;

    private Pcap(int mediaLow, int mediaHigh) {
        this.mediaLow = mediaLow;
        this.mediaHigh = mediaHigh;
    }

    static Pcap read(Path file, int mediaLow, int mediaHigh) throws IOException {
        Pcap p = new Pcap(mediaLow, mediaHigh);
        ByteBuffer b = ByteBuffer.wrap(Files.readAllBytes(file));
        int magic = b.order(ByteOrder.LITTLE_ENDIAN).getInt(0);
        if (magic != 0xa1b2c3d4 && magic != 0xa1b23c4d) {
            b.order(ByteOrder.BIG_ENDIAN);
            magic = b.getInt(0);
        }
        boolean nanos = magic == 0xa1b23c4d;
        int link = b.getInt(20);
        ByteBuffer net = b.duplicate().order(ByteOrder.BIG_ENDIAN); // packet fields are in network order
        int pos = 24;
        while (pos + 16 <= b.limit()) {
            long sec = Integer.toUnsignedLong(b.getInt(pos));
            long frac = Integer.toUnsignedLong(b.getInt(pos + 4));
            int incl = b.getInt(pos + 8);
            int start = pos + 16;
            pos = start + incl;
            if (pos > b.limit()) {
                break;
            }
            long atMs = sec * 1000 + (nanos ? frac / 1_000_000 : frac / 1000);
            int ip = switch (link) {
                case 1 -> (net.getShort(start + 12) & 0xffff) == 0x0800 ? start + 14 : -1;
                case 113 -> (net.getShort(start + 14) & 0xffff) == 0x0800 ? start + 16 : -1;
                case 276 -> (net.getShort(start) & 0xffff) == 0x0800 ? start + 20 : -1;
                default -> -1;
            };
            if (ip >= 0 && ((net.get(ip) & 0xf0) >> 4) == 4) {
                p.packet(net, ip, start + incl, atMs);
            }
        }
        return p;
    }

    private void packet(ByteBuffer b, int ip, int end, long atMs) {
        int ihl = (b.get(ip) & 0x0f) * 4;
        int proto = b.get(ip + 9) & 0xff;
        int l4 = ip + ihl;
        int src = b.getShort(l4) & 0xffff;
        int dst = b.getShort(l4 + 2) & 0xffff;
        if (proto == 6 && (src == 5066 || dst == 5066)) {
            int off = l4 + ((b.get(l4 + 12) & 0xf0) >> 4) * 4;
            if (off < end) {
                websocket(b, off, end, src, dst, atMs);
            }
        } else if (proto == 17) {
            int off = l4 + 8;
            if (src == 5080 || dst == 5080) {
                byte[] payload = bytes(b, off, end);
                sip(new String(payload, StandardCharsets.UTF_8), "udp", src == 5080 ? "freeswitch->carrier" : "carrier->freeswitch", atMs);
            } else if (inMedia(src) || inMedia(dst)) {
                Flow f = flows.computeIfAbsent(src + ">" + dst, k -> new Flow(src, dst));
                f.packets++;
                int first = end > off ? b.get(off) & 0xff : 0;
                if (end - off >= 12 && (first & 0xc0) == 0x80) {
                    int pt = b.get(off + 1) & 0x7f;
                    if (pt < 64 || pt > 95) { // not RTCP (RFC 5761)
                        f.rtp++;
                        f.payloadTypes.add(pt);
                        f.rtpAtMs.add(atMs);
                    }
                }
            }
        }
    }

    private boolean inMedia(int port) {
        return port >= mediaLow && port <= mediaHigh;
    }

    private void websocket(ByteBuffer b, int off, int end, int src, int dst, long atMs) {
        String key = src + ">" + dst;
        ByteArrayOutputStream acc = streams.computeIfAbsent(key, k -> new ByteArrayOutputStream());
        acc.writeBytes(bytes(b, off, end));
        byte[] buf = acc.toByteArray();
        int i = 0;
        // The HTTP upgrade (GET ... / HTTP/1.1 101) comes first on each connection: skipped.
        if (startsWith(buf, "GET ") || startsWith(buf, "HTTP/")) {
            int endOfHead = indexOf(buf, "\r\n\r\n");
            if (endOfHead < 0) {
                return;
            }
            i = endOfHead + 4;
        }
        while (buf.length - i >= 2) {
            int opcode = buf[i] & 0x0f;
            boolean masked = (buf[i + 1] & 0x80) != 0;
            long len = buf[i + 1] & 0x7f;
            int h = 2;
            if (len == 126) {
                if (buf.length - i < 4) {
                    break;
                }
                len = ((buf[i + 2] & 0xffL) << 8) | (buf[i + 3] & 0xffL);
                h = 4;
            } else if (len == 127) {
                if (buf.length - i < 10) {
                    break;
                }
                len = ByteBuffer.wrap(buf, i + 2, 8).getLong();
                h = 10;
            }
            int maskAt = i + h;
            if (masked) {
                h += 4;
            }
            if (len < 0 || len > 1_000_000) {
                buf = new byte[0]; // lost sync (a capture that began mid-stream): drop what is buffered
                i = 0;
                break;
            }
            if (buf.length - i - h < len) {
                break;
            }
            byte[] payload = new byte[(int) len];
            for (int k = 0; k < len; k++) {
                byte x = buf[i + h + k];
                payload[k] = masked ? (byte) (x ^ buf[maskAt + (k % 4)]) : x;
            }
            if (opcode == 1) {
                sip(new String(payload, StandardCharsets.UTF_8), "ws", dst == 5066 ? "browser->freeswitch" : "freeswitch->browser",
                        atMs);
            }
            i += h + (int) len;
        }
        acc.reset();
        acc.write(buf, i, buf.length - i);
    }

    private void sip(String m, String transport, String direction, long atMs) {
        if (!m.startsWith("SIP/2.0") && !m.matches("(?s)^[A-Z]+ \\S+ SIP/2\\.0.*")) {
            return;
        }
        int split = m.indexOf("\r\n\r\n");
        String head = split < 0 ? m : m.substring(0, split);
        String body = split < 0 ? "" : m.substring(split + 4);
        List<String> lines = head.lines().toList();
        List<String> iids = new ArrayList<>();
        List<String> legs = new ArrayList<>();
        String callId = null;
        String cseq = null;
        String se = null;
        for (String l : lines.subList(1, lines.size())) {
            int c = l.indexOf(':');
            if (c <= 0) {
                continue;
            }
            String n = l.substring(0, c).trim().toLowerCase(Locale.ROOT);
            String v = l.substring(c + 1).trim();
            switch (n) {
                case "call-id", "i" -> callId = v;
                case "cseq" -> cseq = v;
                case "x-parity-interaction-id" -> iids.add(v);
                case "x-parity-leg-id" -> legs.add(v);
                case "session-expires", "x" -> se = v;
                default -> {
                    // not reported
                }
            }
        }
        String media = body.lines().filter(l -> l.startsWith("m=")).findFirst().orElse(null);
        sip.add(new Sip(atMs, transport, direction, lines.get(0), callId, cseq, iids, legs, se, media));
    }

    private static boolean startsWith(byte[] buf, String prefix) {
        return buf.length >= prefix.length()
                && new String(buf, 0, prefix.length(), StandardCharsets.ISO_8859_1).equals(prefix);
    }

    private static int indexOf(byte[] buf, String what) {
        return new String(buf, StandardCharsets.ISO_8859_1).indexOf(what);
    }

    private static byte[] bytes(ByteBuffer b, int from, int to) {
        byte[] out = new byte[Math.max(0, to - from)];
        b.get(from, out);
        return out;
    }
}
