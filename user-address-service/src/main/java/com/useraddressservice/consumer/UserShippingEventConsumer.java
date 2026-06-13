package com.useraddressservice.consumer;

import com.springbootkafka.difc.DifcOps;
import com.springbootkafka.difc.DifcTags;
import com.useraddressservice.difc.DifcConsumerLifecycle;
import com.useraddressservice.entity.Address;
import com.useraddressservice.service.AddressService;
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
public class UserShippingEventConsumer {

    private final AddressService addressService;

    @KafkaListener(
            id = DifcConsumerLifecycle.LISTENER_ID,
            topics = "${kafka.topics.user-created.topic}",
            groupId = "${kafka.topics.user-created.consumerGroup}",
            containerFactory = "concurrentKafkaListenerContainerFactory",
            autoStartup = "false"
    )
    public void consumeShippingEvent(
            @Payload(required = false) final UserShippingEvent event,
            final ConsumerRecord<String, Object> consumerRecord,
            final Consumer<?, ?> consumer) {
        DifcOps.refreshConsumerLabel(log, DifcTags.PRINCIPAL_USER_ADDRESS_SERVICE, consumer, DifcTags.USER_SHIPPING);

        if (event == null) {
            System.out.printf(
                    "[AddressService] Skipping record (not authorized for tag=%s) partition=%d offset=%d key=%s%n",
                    DifcTags.USER_CONTACT, consumerRecord.partition(), consumerRecord.offset(), consumerRecord.key());
            log.debug("[AddressService] Skipping non-shipping record partition={} offset={}",
                    consumerRecord.partition(), consumerRecord.offset());
            return;
        }

        System.out.printf(
                "[AddressService] Consumed user-shipping userId=%s address=%s partition=%d offset=%d%n",
                event.getUserId(), event.getAddressText(), consumerRecord.partition(), consumerRecord.offset());
        log.info("[AddressService] Consumed user-shipping userId={} address={} partition={} offset={}",
                event.getUserId(), event.getAddressText(), consumerRecord.partition(), consumerRecord.offset());

        final Address entity = UserShippingEvent.toAddressEntity(event);
        addressService.saved(entity);
        System.out.printf("[AddressService] Saved address for userId=%s%n", entity.getUserId());
        log.info("[AddressService] Saved address for userId={}", entity.getUserId());
    }
}
