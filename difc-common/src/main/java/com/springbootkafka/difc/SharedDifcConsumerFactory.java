package com.springbootkafka.difc;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.Map;

/**
 * Returns one {@link KafkaConsumer} for DIFC bootstrap ({@code GRANT_CAP}, {@code addTag}) and
 * for the Spring {@code @KafkaListener} container.
 */
public class SharedDifcConsumerFactory<K, V> extends DefaultKafkaConsumerFactory<K, V> {

    private static final Logger log = LoggerFactory.getLogger(SharedDifcConsumerFactory.class);

    private final String serviceName;
    private final String logLabel;
    private volatile Consumer<K, V> sharedConsumer;
    private final Object consumerLock = new Object();

    public SharedDifcConsumerFactory(
            final Map<String, Object> configs,
            final String serviceName,
            final String logLabel) {
        super(configs);
        this.serviceName = serviceName;
        this.logLabel = logLabel;
    }

    @Override
    protected Consumer<K, V> createKafkaConsumer(final Map<String, Object> configProps) {
        synchronized (consumerLock) {
            if (sharedConsumer == null) {
                sharedConsumer = super.createKafkaConsumer(configProps);
                if (sharedConsumer instanceof KafkaConsumer<?, ?> kafkaConsumer) {
                    DifcOps.registerConsumer(log, serviceName, kafkaConsumer);
                    System.out.printf(
                            "[%s] DIFC shared consumer ready (clientId=%s)%n",
                            logLabel,
                            serviceName);
                }
            }
            return sharedConsumer;
        }
    }
}
