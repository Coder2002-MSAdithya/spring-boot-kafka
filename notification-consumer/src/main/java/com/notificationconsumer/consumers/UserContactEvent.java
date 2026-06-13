package com.notificationconsumer.consumers;

import com.notificationconsumer.entity.Notification;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/** Contact payload (DIFC tag user-contact). Contains no street address. */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class UserContactEvent {
    private Long userId;
    private String firstName;
    private String lastName;
    private String email;

    public static Notification toNotification(final UserContactEvent event) {
        return Notification.builder()
                .userId(event.getUserId())
                .email(event.getEmail())
                .isSend(Boolean.TRUE)
                .build();
    }
}
