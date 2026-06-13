package com.springbootkafka.difc;

/**
 * DIFC labels for the Spring Boot user-registration flow.
 * <ul>
 *   <li>{@link #USER_CONTACT} — name and email for notification-service</li>
 *   <li>{@link #USER_SHIPPING} — delivery address for user-address-service</li>
 * </ul>
 */
public final class DifcTags {

    /** PII suitable for welcome / contact notifications (no street address). */
    public static final String USER_CONTACT = "user-contact";

    /** Shipping / delivery address (no email or full name). */
    public static final String USER_SHIPPING = "user-shipping";

    public static final String TOPIC_USER_CREATED = "user-service.user_created.1";

    public static final String PRINCIPAL_USER_SERVICE = "user-svc";
    public static final String PRINCIPAL_USER_ADDRESS_SERVICE = "user-address-svc";
    public static final String PRINCIPAL_NOTIFICATION_SERVICE = "notification-svc";

    private DifcTags() {
    }
}
