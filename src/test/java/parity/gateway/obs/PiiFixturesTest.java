package parity.gateway.obs;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

/**
 * parity-contracts' cross-language masking cases ({@code masking/cases.v1.json}), the same
 * cases interaction-engine's {@code ie-obs} must pass: the Java rules can't drift from the
 * Rust ones. The file is required; a missing parity-contracts fails the test.
 */
class PiiFixturesTest {

    private static Map<String, Object> cases;

    @BeforeAll
    @SuppressWarnings("unchecked")
    static void load() throws IOException {
        Path dir = Path.of(System.getProperty("parity.contracts.dir", "../parity-contracts"));
        Path file = dir.resolve("masking/cases.v1.json");
        assertThat(file).as("parity-contracts' masking cases").exists();
        cases = JsonMapper.builder().build().readValue(Files.readString(file), Map.class);
        assertThat(cases.get("schema_version")).isEqualTo(1);
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> list(String key) {
        return (List<Map<String, Object>>) cases.get(key);
    }

    @Test
    void namesAreNormalizedAsInIeObs() {
        for (Map<String, Object> c : list("normalize")) {
            assertThat(Pii.normalize((String) c.get("in"))).as((String) c.get("in")).isEqualTo(c.get("out"));
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void everyNameHasIeObsClass() {
        Map<String, List<String>> byClass = (Map<String, List<String>>) cases.get("classify");
        assertThat(byClass).containsOnlyKeys("phone", "secret", "plain");
        int n = 0;
        for (var e : byClass.entrySet()) {
            Pii.FieldClass expected = Pii.FieldClass.valueOf(e.getKey().toUpperCase(java.util.Locale.ROOT));
            for (String name : e.getValue()) {
                assertThat(Pii.classify(name)).as(name).isEqualTo(expected);
                n++;
            }
        }
        assertThat(n).isGreaterThan(50);
    }

    @Test
    void phonesKeepOnlyTheirLastThreeDigits() {
        for (Map<String, Object> c : list("mask_phone")) {
            assertThat(Pii.maskPhone((String) c.get("in"))).as((String) c.get("in")).isEqualTo(c.get("out"));
        }
    }

    @Test
    void structuredValuesAreMaskedAtEveryDepth() {
        for (Map<String, Object> c : list("mask_structured")) {
            assertThat(Pii.maskNested(c.get("in"))).isEqualTo(c.get("out"));
        }
    }
}
