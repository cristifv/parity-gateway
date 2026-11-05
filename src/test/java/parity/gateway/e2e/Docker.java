package parity.gateway.e2e;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** The docker CLI for the real-node test; its containers carry ci.sh's label, so a leftover is removed. */
final class Docker {

    static final String LABEL = "parity.test=parity-gateway";

    private Docker() {}

    static String run(List<String> args, boolean tolerant) throws IOException, InterruptedException {
        List<String> cmd = new ArrayList<>(List.of("docker"));
        cmd.addAll(args);
        Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
        String output = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        if (!p.waitFor(180, TimeUnit.SECONDS)) {
            p.destroyForcibly();
            throw new IOException("docker timed out");
        }
        if (p.exitValue() != 0 && !tolerant) {
            throw new IOException("docker " + args.get(0) + " failed: " + output.lines().limit(5).toList());
        }
        return output;
    }

    static void rm(String... names) throws IOException, InterruptedException {
        List<String> args = new ArrayList<>(List.of("rm", "-f"));
        args.addAll(List.of(names));
        run(args, true);
    }

    /** Names of running containers that look like another real-node run (TC's tests, IE's demo, this test). */
    static List<String> others() throws IOException, InterruptedException {
        return run(List.of("ps", "--format", "{{.Names}}"), false).lines()
                .filter(n -> n.startsWith("tc-test-") || n.startsWith("ie-demo-") || n.startsWith("gw-e2e-")).toList();
    }
}
