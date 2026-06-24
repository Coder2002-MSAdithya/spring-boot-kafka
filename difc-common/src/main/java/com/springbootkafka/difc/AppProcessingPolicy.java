package com.springbootkafka.difc;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Processing policy exported by the DIFC policy Java agent ({@code processing-policy.json}).
 *
 * <p>Minimal model for the {@code spring-boot-kafka} grantor: only fields used for
 * external-connection verification on {@code CAN_ADD} are modeled explicitly; other
 * agent-emitted fields are ignored for forward compatibility.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class AppProcessingPolicy {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private String principal;
    private String service;
    private List<ExternalConnection> externalConnections = new ArrayList<>();

    public String getPrincipal() {
        return principal;
    }

    public String getService() {
        return service;
    }

    public List<ExternalConnection> getExternalConnections() {
        return externalConnections == null ? Collections.emptyList() : externalConnections;
    }

    public static AppProcessingPolicy readFromCanonicalJson(final String canonicalJson) throws IOException {
        return MAPPER.readValue(canonicalJson, AppProcessingPolicy.class);
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static final class ExternalConnection {
        private String endpoint;
        private String protocol;

        public String getEndpoint() {
            return endpoint;
        }

        public String getProtocol() {
            return protocol;
        }
    }
}
