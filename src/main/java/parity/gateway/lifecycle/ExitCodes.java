package parity.gateway.lifecycle;

import org.springframework.boot.context.config.ConfigDataException;
import org.springframework.boot.context.properties.ConfigurationPropertiesBindException;
import org.springframework.boot.context.properties.bind.BindException;
import org.springframework.boot.context.properties.bind.validation.BindValidationException;
import org.yaml.snakeyaml.error.YAMLException;

/** The process's exit codes, documented on the application class. */
public final class ExitCodes {

    public static final int DRAINED = 0;
    public static final int FAILED = 1;
    public static final int CONFIGURATION = 2;
    public static final int NODE_LOCKED = 3;

    private ExitCodes() {}

    /** The code for a start that failed, from the cause chain. */
    public static int forStartupFailure(Throwable failure) {
        for (Throwable t = failure; t != null; t = t.getCause()) {
            if (t instanceof ConfigurationPropertiesBindException
                    || t instanceof BindException
                    || t instanceof BindValidationException
                    || t instanceof ConfigDataException
                    || t instanceof YAMLException
                    || t instanceof ConfigurationRefusedException) {
                return CONFIGURATION;
            }
            if (sameThrowable(t.getCause(), t)) {
                break;
            }
        }
        return FAILED;
    }

    /** A second signal ends the process at once, the shell's way: 128 + the signal's number. */
    public static int forSignal(int signalNumber) {
        return 128 + signalNumber;
    }

    /** Identity on purpose: a throwable that is its own cause ends the walk. */
    @SuppressWarnings("ReferenceEquality")
    private static boolean sameThrowable(Throwable a, Throwable b) {
        return a == b;
    }
}
