package parity.gateway.obs;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Every event name the code writes is in {@code logging/events.parity-gateway.v1.json} or the
 * common catalogue, and every entry of the service's catalogue is written somewhere: the
 * catalogue is the list dashboards and the k3s checker key on (log-record.v1). The captured
 * test logs are checked by contract-check in scripts/ci.sh; this catches the call sites no test
 * reaches.
 */
class LogCatalogueTest {

    static final String SERVICE = "parity-gateway";
    /** The event literals: {@code "event", "x.y"} and the session helpers {@code at(log.atX(), "x.y"} / {@code call(...)}. */
    static final Pattern SITE = Pattern.compile(
            "(?:\"event\",\\s*|\\b(?:at|call)\\(\\w+\\.at\\w+\\(\\),\\s*|line\\(\"\\w+\",\\s*|direct\\(\"\\w+\",\\s*)\"([a-z0-9_.]+)\"");
    /** A plain slf4j call names no event: every record of this service goes through the fluent API. */
    static final Pattern PLAIN = Pattern.compile("\\b(?:log|LOG|logger)\\.(?:trace|debug|info|warn|error)\\(");
    /** Written by the formatter itself. */
    static final Set<String> FORMATTER = Set.of("log.external");

    private static Set<String> names(Path catalogue) throws IOException {
        JsonNode doc = JsonMapper.builder().build().readTree(Files.readString(catalogue));
        Set<String> names = new TreeSet<>();
        doc.path("events").properties().forEach(e -> names.add(e.getKey()));
        return names;
    }

    @Test
    void codeAndCatalogueAgree() throws IOException {
        Path contracts = Path.of(System.getProperty("parity.contracts.dir", "../parity-contracts"));
        Set<String> own = names(Path.of("logging/events." + SERVICE + ".v1.json"));
        Set<String> common = names(contracts.resolve("logging/events.common.v1.json"));
        assertThat(own).as("the service's catalogue repeats no common event").doesNotContainAnyElementsOf(common);
        Set<String> used = new TreeSet<>();
        Set<String> plain = new TreeSet<>();
        try (Stream<Path> files = Files.walk(Path.of("src/main/java"))) {
            for (Path f : files.filter(p -> p.toString().endsWith(".java")).toList()) {
                String text = Files.readString(f);
                Matcher m = SITE.matcher(text);
                while (m.find()) {
                    used.add(m.group(1));
                }
                if (PLAIN.matcher(text).find()) {
                    plain.add(f.toString());
                }
            }
        }
        assertThat(plain).as("plain log calls (no event name)").isEmpty();
        Set<String> unknown = new TreeSet<>(used);
        unknown.removeAll(own);
        unknown.removeAll(common);
        assertThat(unknown).as("events the code writes that no catalogue lists").isEmpty();
        Set<String> unused = new TreeSet<>(own);
        unused.removeAll(used);
        unused.removeAll(FORMATTER);
        assertThat(unused).as("catalogue entries no code writes").isEmpty();
        assertThat(new HashSet<>(used)).hasSizeGreaterThan(10);
    }
}
