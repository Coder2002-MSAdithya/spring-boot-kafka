package com.userservice.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.springbootkafka.difc.DifcOps;
import com.springbootkafka.difc.DifcTags;
import com.userservice.dto.UserContactEvent;
import com.userservice.dto.UserShippingEvent;
import com.userservice.entity.User;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Set;

@Component
@RequiredArgsConstructor
@Slf4j
public class DifcUserEventProducer {

    private final KafkaProducer<String, String> producer;
    private final ObjectMapper objectMapper;

    @Value("${kafka.topics.user-created.topicName}")
    private String topicName;

    @PostConstruct
    void bootstrapDifc() {
        DifcOps.bootstrapProducerTags(log, DifcTags.PRINCIPAL_USER_SERVICE, producer);
        System.out.printf(
                "[UserService] DIFC tags ready: %s (contact/notification), %s (shipping/address) on topic=%s%n",
                DifcTags.USER_CONTACT, DifcTags.USER_SHIPPING, topicName);
        log.info("[UserService] DIFC tags ready on topic={}", topicName);
    }

    public void publishUserRegistration(final User user, final String addressText) {
        publishContact(UserContactEvent.from(user));
        publishShipping(UserShippingEvent.from(user.getId(), addressText));
        producer.flush();
    }

    private void publishContact(final UserContactEvent event) {
        sendTagged(
                event.getUserId().toString(),
                event,
                Set.of(DifcTags.USER_CONTACT),
                String.format(
                        "[UserService] Publishing user-contact userId=%s email=%s tag=%s%n",
                        event.getUserId(), event.getEmail(), DifcTags.USER_CONTACT));
    }

    private void publishShipping(final UserShippingEvent event) {
        sendTagged(
                event.getUserId().toString(),
                event,
                Set.of(DifcTags.USER_SHIPPING),
                String.format(
                        "[UserService] Publishing user-shipping userId=%s address=%s tag=%s%n",
                        event.getUserId(), event.getAddressText(), DifcTags.USER_SHIPPING));
    }

    private void sendTagged(
            final String key,
            final Object payload,
            final Set<String> tags,
            final String stdoutLine) {
        final String json = toJson(payload);
        final ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, json);
        System.out.print(stdoutLine);
        log.info(stdoutLine.trim());

        producer.sendWithTags(record, tags, (final RecordMetadata metadata, final Exception ex) -> {
            if (ex != null) {
                System.out.printf("[UserService] sendWithTags FAILED key=%s tags=%s error=%s%n", key, tags, ex.getMessage());
                log.error("[UserService] sendWithTags failed key={} tags={}", key, tags, ex);
                return;
            }
            System.out.printf(
                    "[UserService] sendWithTags OK key=%s tags=%s partition=%d offset=%d%n",
                    key, tags, metadata.partition(), metadata.offset());
            log.info("[UserService] sendWithTags OK key={} tags={} partition={} offset={}",
                    key, tags, metadata.partition(), metadata.offset());
        });
    }

    private String toJson(final Object payload) {
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (final JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize " + payload.getClass().getSimpleName(), e);
        }
    }
}
