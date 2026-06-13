package com.userservice.producer;

import com.userservice.entity.User;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class KafkaProducer {

    private final DifcUserEventProducer difcUserEventProducer;

    public void publishUserRegistration(final User user, final String addressText) {
        difcUserEventProducer.publishUserRegistration(user, addressText);
    }
}
