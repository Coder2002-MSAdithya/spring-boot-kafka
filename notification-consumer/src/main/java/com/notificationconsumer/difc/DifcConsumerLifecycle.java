package com.notificationconsumer.difc;

import com.springbootkafka.difc.DifcConsumerStartupGate;
import com.springbootkafka.difc.DifcTags;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

@Configuration
@EnableScheduling
@RequiredArgsConstructor
@Slf4j
public class DifcConsumerLifecycle {

    public static final String LISTENER_ID = "userCreatedListener";
    private static final String LOG_LABEL = "NotificationService";

    private final KafkaListenerEndpointRegistry registry;
    private final ConsumerFactory<String, ?> consumerFactory;
    private DifcConsumerStartupGate gate;

    @PostConstruct
    void initGate() {
        gate = new DifcConsumerStartupGate(
                log,
                LOG_LABEL,
                LISTENER_ID,
                registry,
                consumerFactory,
                DifcTags.USER_CONTACT);
    }

    @Scheduled(fixedDelay = 2000, initialDelay = 2000)
    void tryStartConsuming() {
        if (gate != null) {
            gate.tryStartListener();
        }
    }
}
