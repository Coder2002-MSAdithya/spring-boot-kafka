package com.springbootkafka.difc;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.MessageListenerContainer;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Starts a Kafka listener on the same shared consumer used for DIFC {@code GRANT_CAP} and label setup.
 */
public final class DifcConsumerStartupGate {

    private final Logger log;
    private final String logLabel;
    private final String listenerId;
    private final KafkaListenerEndpointRegistry registry;
    private final ConsumerFactory<String, ?> consumerFactory;
    private final String[] labelTags;
    private final AtomicBoolean grantCapRequested = new AtomicBoolean(false);
    private final AtomicBoolean started = new AtomicBoolean(false);

    public DifcConsumerStartupGate(
            final Logger log,
            final String logLabel,
            final String listenerId,
            final KafkaListenerEndpointRegistry registry,
            final ConsumerFactory<String, ?> consumerFactory,
            final String... labelTags) {
        this.log = log;
        this.logLabel = logLabel;
        this.listenerId = listenerId;
        this.registry = registry;
        this.consumerFactory = consumerFactory;
        this.labelTags = labelTags;
    }

    public void tryStartListener() {
        if (started.get()) {
            return;
        }
        final MessageListenerContainer container = registry.getListenerContainer(listenerId);
        if (container == null) {
            return;
        }

        final Consumer<?, ?> consumer = consumerFactory.createConsumer();
        if (!(consumer instanceof KafkaConsumer<?, ?> kafkaConsumer)) {
            return;
        }

        if (!grantCapRequested.get()) {
            try {
                DifcPrivilegeRequests.requestGrantCapForConsume(log, logLabel, kafkaConsumer, labelTags[0]);
                grantCapRequested.set(true);
            } catch (final RuntimeException e) {
                log.warn("[{}] GRANT_CAP not yet accepted: {}", logLabel, e.getMessage());
                return;
            }
        }

        if (!DifcPrivilegeRequests.tryApplyConsumerLabels(log, logLabel, kafkaConsumer, labelTags)) {
            return;
        }

        if (!container.isRunning()) {
            container.start();
        }
        started.set(true);
        System.out.printf("[%s] DIFC labels applied on shared consumer; listener '%s' consuming%n",
                logLabel, listenerId);
        log.info("[{}] DIFC labels applied on shared consumer; listener '{}' consuming", logLabel, listenerId);
    }
}
