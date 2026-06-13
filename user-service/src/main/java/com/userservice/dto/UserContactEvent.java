package com.userservice.dto;

import com.userservice.entity.User;
import lombok.Builder;
import lombok.Data;

/** Contact details for notification-service (DIFC tag {@code user-contact}). */
@Data
@Builder
public class UserContactEvent {

    private Long userId;
    private String firstName;
    private String lastName;
    private String email;

    public static UserContactEvent from(final User user) {
        return UserContactEvent.builder()
                .userId(user.getId())
                .firstName(user.getFirstName())
                .lastName(user.getLastName())
                .email(user.getEmail())
                .build();
    }
}
