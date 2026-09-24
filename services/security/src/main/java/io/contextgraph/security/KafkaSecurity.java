package io.contextgraph.security;
import java.nio.file.*;
import java.util.*;
public final class KafkaSecurity {
    private KafkaSecurity() {}
    public static Properties properties() {
        String path=System.getenv("KAFKA_CLIENT_CONFIG");
        if(path==null || path.isBlank()) throw new IllegalStateException("KAFKA_CLIENT_CONFIG is required");
        return properties(Path.of(path));
    }
    public static Properties properties(Path path) {
        Properties p=new Properties();
        try(var input=Files.newInputStream(path)) { p.load(input); }
        catch(Exception e) { throw new IllegalStateException("Cannot load Kafka client credentials"); }
        if(!"SASL_SSL".equals(p.getProperty("security.protocol"))) throw new IllegalStateException("Kafka requires SASL_SSL");
        if(!"https".equalsIgnoreCase(p.getProperty("ssl.endpoint.identification.algorithm","https"))) throw new IllegalStateException("Kafka TLS hostname verification is mandatory");
        if(!Set.of("SCRAM-SHA-256","SCRAM-SHA-512","PLAIN").contains(p.getProperty("sasl.mechanism","")) || p.getProperty("sasl.jaas.config","").isBlank()) throw new IllegalStateException("Kafka SASL credentials are required");
        p.setProperty("ssl.endpoint.identification.algorithm","https");
        return p;
    }
}
