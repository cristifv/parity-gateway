package parity.gateway.lifecycle;

/**
 * A configuration the controller refuses to run with (exit code 2), such as a test-only
 * setting outside the {@code test} profile. Its message names the setting, never a value that
 * could hold data.
 */
public class ConfigurationRefusedException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public ConfigurationRefusedException(@com.google.errorprone.annotations.CompileTimeConstant String message) {
        super(message);
    }
}
