package parity.gateway.obs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class PhoneTest {

    @Test
    void aPhonePrintsMaskedAndParsesOnlyE164() {
        Phone p = Phone.parseE164("+447700900123");
        assertThat(p.toString()).isEqualTo("***123");
        assertThat(p.expose()).isEqualTo("+447700900123");
        for (String bad : List.of("447700900123", "+0447700900", "+44 7700", "+12345")) {
            assertThatThrownBy(() -> Phone.parseE164(bad)).as(bad)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageNotContaining("4477");
        }
    }

    @Test
    void aPhoneUnderAnyNameIsMasked() {
        Phone p = Phone.parseE164("+447700900123");
        assertThat(Pii.maskField("note", p)).isEqualTo("***123");
        assertThat(Pii.maskField("to", p)).isEqualTo("***123");
        assertThat(Pii.maskNested(Map.of("x", List.of(p)))).isEqualTo(Map.of("x", List.of("***123")));
    }
}
