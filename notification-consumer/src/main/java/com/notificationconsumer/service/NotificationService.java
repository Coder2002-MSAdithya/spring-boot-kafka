package com.notificationconsumer.service;

import com.notificationconsumer.consumers.UserContactEvent;
import com.notificationconsumer.entity.Notification;
import com.notificationconsumer.repository.NotificationRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
@Slf4j
public class NotificationService {

    private final Optional<NotificationRepository> notificationRepository;

    @Autowired
    public NotificationService(@Autowired(required = false) final NotificationRepository notificationRepository) {
        this.notificationRepository = Optional.ofNullable(notificationRepository);
    }

    public void notifyUserContact(final UserContactEvent event) {
        final String message = String.format(
                "Sending welcome email to %s %s (%s)",
                event.getFirstName(),
                event.getLastName(),
                event.getEmail());
        System.out.printf("[NotificationService] %s%n", message);
        log.info("[NotificationService] {}", message);

        notificationRepository.ifPresent(repo -> {
            final Notification entity = UserContactEvent.toNotification(event);
            final Notification saved = repo.save(entity);
            System.out.printf("[NotificationService] Persisted notification id=%s for userId=%s%n",
                    saved.getId(), event.getUserId());
            log.info("[NotificationService] Persisted notification id={} for userId={}",
                    saved.getId(), event.getUserId());
        });
    }
}
