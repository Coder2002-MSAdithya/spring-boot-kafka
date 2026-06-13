package com.notificationconsumer.config;

import com.springbootkafka.difc.DifcTags;
import com.springbootkafka.difc.SharedDifcConsumerFactory;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.support.converter.StringJsonMessageConverter;

import java.util.HashMap;
import java.util.Map;

@Configuration
@RequiredArgsConstructor
public class UserCreatedKafkaConsumerConfig<T> {

    private final Environment environment;

    @Value("${kafka.topics.user-created.consumerGroup}")
    private String consumerGroup;

    @Bean
    public ConsumerFactory<String, T> consumerFactory() {
        return new SharedDifcConsumerFactory<>(
                consumerProperties(),
                DifcTags.PRINCIPAL_NOTIFICATION_SERVICE,
                "NotificationService");
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, T> concurrentKafkaListenerContainerFactory() {
        final ConcurrentKafkaListenerContainerFactory<String, T> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setRecordMessageConverter(new StringJsonMessageConverter());
        return factory;
    }

    private Map<String, Object> consumerProperties() {
        final Map<String, Object> config = new HashMap<>();
        put(config, ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "bootstrap.servers", "kafka.host");
        put(config, ConsumerConfig.GROUP_ID_CONFIG, "group.id", "kafka.topics.user-created.consumerGroup");
        put(config, ConsumerConfig.CLIENT_ID_CONFIG, "client.id", "kafka.client-id");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        put(config, "security.protocol", "security.protocol", null);
        put(config, "sasl.mechanism", "sasl.mechanism", null);
        put(config, "sasl.jaas.config", "sasl.jaas.config", null);
        if (!config.containsKey(ConsumerConfig.GROUP_ID_CONFIG)) {
            config.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        }
        return config;
    }

    private void put(final Map<String, Object> config, final String targetKey, final String primaryKey, final String fallbackKey) {
        String value = environment.getProperty(primaryKey);
        if (value == null && fallbackKey != null) {
            value = environment.getProperty(fallbackKey);
        }
        if (value != null) {
            config.put(targetKey, value);
        }
    }
}
