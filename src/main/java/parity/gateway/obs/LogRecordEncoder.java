package parity.gateway.obs;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.encoder.EncoderBase;
import java.nio.charset.StandardCharsets;

/**
 * The logback encoder of log-record.v1 lines ({@link JsonLogFormatter}). Its identity comes from
 * the logback configuration: {@code logback-spring.xml} reads it from the Spring properties
 * ({@code <springProperty>}), the tests' {@code logback-test.xml} names it.
 */
public class LogRecordEncoder extends EncoderBase<ILoggingEvent> {

    private static final byte[] NONE = new byte[0];

    private String service = "parity-gateway";
    private String version = "unknown";
    private String instance = "";
    private String zone = "";
    private String bin = "";
    private JsonLogFormatter formatter;

    public void setService(String service) {
        this.service = service;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public void setInstance(String instance) {
        this.instance = instance;
    }

    public void setZone(String zone) {
        this.zone = zone;
    }

    public void setBin(String bin) {
        this.bin = bin;
    }

    @Override
    public void start() {
        JsonLogFormatter.Identity id = new JsonLogFormatter.Identity(service, version, instance, zone, bin);
        formatter = new JsonLogFormatter(id);
        JsonLogFormatter.remember(id);
        super.start();
    }

    @Override
    public byte[] headerBytes() {
        return NONE;
    }

    @Override
    public byte[] encode(ILoggingEvent event) {
        return formatter.format(event).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public byte[] footerBytes() {
        return NONE;
    }
}
