package parity.gateway.e2e;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * One freeswitch ESL command, for the negative cases only (calls TC never made: no
 * {@code cc_managed}, so TC leaves them alone). The password is read from the secret file and
 * never put on a command line.
 */
final class Esl {

    private Esl() {}

    /** Sends {@code bgapi <command>} and returns freeswitch's reply line (a Job-UUID). */
    static String bgapi(String password, String command) throws IOException {
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), 8021), 2000);
            s.setSoTimeout(5000);
            BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
            OutputStream out = s.getOutputStream();
            String first = block(in);
            if (!first.contains("auth/request")) {
                throw new IOException("no ESL auth request");
            }
            out.write(("auth " + password + "\n\n").getBytes(StandardCharsets.UTF_8));
            if (!block(in).contains("+OK")) {
                throw new IOException("ESL auth refused");
            }
            out.write(("bgapi " + command + "\n\n").getBytes(StandardCharsets.UTF_8));
            String reply = block(in);
            out.write("exit\n\n".getBytes(StandardCharsets.UTF_8));
            return reply.lines().filter(l -> l.startsWith("Reply-Text:")).findFirst().orElse(reply.strip());
        }
    }

    private static String block(BufferedReader in) throws IOException {
        StringBuilder b = new StringBuilder();
        for (String line = in.readLine(); line != null && !line.isEmpty(); line = in.readLine()) {
            b.append(line).append('\n');
        }
        return b.toString();
    }
}
