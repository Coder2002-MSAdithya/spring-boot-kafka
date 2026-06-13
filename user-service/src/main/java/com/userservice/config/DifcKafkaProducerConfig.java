package com.userservice.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import java.util.HashMap;
import java.util.Map;

@Configuration
@RequiredArgsConstructor
@Slf4j
public class DifcKafkaProducerConfig {

    private final Environment environment;

    @Bean(destroyMethod = "close")
    public KafkaProducer<String, String> difcKafkaProducer() {
        return new KafkaProducer<>(producerProperties());
    }

    private Map<String, Object> producerProperties() {
        final Map<String, Object> config = new HashMap<>();
        putIfPresent(config, ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "bootstrap.servers", "kafka.host");
        putIfPresent(config, ProducerConfig.CLIENT_ID_CONFIG, "client.id", "kafka.client-id");
        putIfPresent(config, ProducerConfig.ACKS_CONFIG, "acks", null);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        putIfPresent(config, "security.protocol", "security.protocol", null);
        putIfPresent(config, "sasl.mechanism", "sasl.mechanism", null);
        putIfPresent(config, "sasl.jaas.config", "sasl.jaas.config", null);
        putIfPresent(config, ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "request.timeout.ms", null);
        putIfPresent(config, ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "delivery.timeout.ms", null);
        putIfPresent(config, ProducerConfig.MAX_BLOCK_MS_CONFIG, "max.block.ms", null);
        if (!config.containsKey(ProducerConfig.ACKS_CONFIG)) {
            config.put(ProducerConfig.ACKS_CONFIG, "all");
        }
        // Idempotent producer init can race a freshly started cluster and kill the sender thread.
        config.putIfAbsent(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        return config;
    }

    private void putIfPresent(final Map<String, Object> config, final String targetKey, final String primaryKey, final String fallbackKey) {
        String value = environment.getProperty(primaryKey);
        if (value == null && fallbackKey != null) {
            value = environment.getProperty(fallbackKey);
        }
        if (value != null) {
            if (targetKey.endsWith("_ms") || targetKey.contains("timeout") || targetKey.contains("max.block")) {
                config.put(targetKey, Integer.parseInt(value));
            } else {
                config.put(targetKey, value);
            }
        }
    }
}
