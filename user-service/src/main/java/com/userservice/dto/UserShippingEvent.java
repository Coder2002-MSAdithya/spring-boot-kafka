package com.userservice.dto;

import lombok.Builder;
import lombok.Data;

/** Shipping address for user-address-service (DIFC tag {@code user-shipping}). */
@Data
@Builder
public class UserShippingEvent {

    private Long userId;
    private String addressText;

    public static UserShippingEvent from(final Long userId, final String addressText) {
        return UserShippingEvent.builder()
                .userId(userId)
                .addressText(addressText)
                .build();
    }
}
