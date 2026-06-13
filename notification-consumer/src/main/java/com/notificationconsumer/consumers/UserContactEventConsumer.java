package com.notificationconsumer.consumers;

import com.notificationconsumer.service.NotificationService;
import com.springbootkafka.difc.DifcOps;
import com.springbootkafka.difc.DifcTags;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class UserContactEventConsumer {

    private final NotificationService notificationService;

    @KafkaListener(
            id = com.notificationconsumer.difc.DifcConsumerLifecycle.LISTENER_ID,
            topics = "${kafka.topics.user-created.topic}",
            groupId = "${kafka.topics.user-created.consumerGroup}",
            containerFactory = "concurrentKafkaListenerContainerFactory",
            autoStartup = "false"
    )
    public void consumeContactEvent(
            @Payload(required = false) final UserContactEvent event,
            final ConsumerRecord<String, Object> consumerRecord,
            final Consumer<?, ?> consumer) {
        DifcOps.refreshConsumerLabel(log, DifcTags.PRINCIPAL_NOTIFICATION_SERVICE, consumer, DifcTags.USER_CONTACT);

        if (event == null) {
            System.out.printf(
                    "[NotificationService] Skipping record (not authorized for tag=%s) partition=%d offset=%d key=%s%n",
                    DifcTags.USER_SHIPPING, consumerRecord.partition(), consumerRecord.offset(), consumerRecord.key());
            log.debug("[NotificationService] Skipping non-contact record partition={} offset={}",
                    consumerRecord.partition(), consumerRecord.offset());
            return;
        }

        System.out.printf(
                "[NotificationService] Consumed user-contact userId=%s email=%s partition=%d offset=%d%n",
                event.getUserId(), event.getEmail(), consumerRecord.partition(), consumerRecord.offset());
        log.info("[NotificationService] Consumed user-contact userId={} email={} partition={} offset={}",
                event.getUserId(), event.getEmail(), consumerRecord.partition(), consumerRecord.offset());

        notificationService.notifyUserContact(event);
    }
}
