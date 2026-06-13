package com.useraddressservice.consumer;

import com.useraddressservice.entity.Address;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/** Shipping payload (DIFC tag user-shipping). Contains no email or name. */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class UserShippingEvent {
    private Long userId;
    private String addressText;

    public static Address toAddressEntity(final UserShippingEvent event) {
        return Address.builder()
                .userId(event.getUserId())
                .addressText(event.getAddressText())
                .build();
    }
}
