package com.springbootkafka.difc;

import org.apache.kafka.clients.Capability;

/**
 * Allow-list for tag owners approving {@code GRANT_CAP} requests via {@code ADD_CLIENT_PRIVS}.
 * Requester identity is the authenticated SCRAM principal recorded by the broker on {@code GRANT_CAP}.
 *
 * <p>Downstream services only consume tagged slices; none declassify on produce — {@code CAN_ADD} only.
 */
public final class DifcGrantPolicy {

    private DifcGrantPolicy() {
    }

    public static boolean isAllowedGrant(
            final String tagName,
            final String requesterPrincipal,
            final Capability capability) {
        if (tagName == null || requesterPrincipal == null || capability == null) {
            return false;
        }
        return switch (capability) {
            case CAN_ADD -> isAllowedAddGrant(tagName, requesterPrincipal);
            case CAN_REMOVE -> false;
        };
    }

    private static boolean isAllowedAddGrant(final String tagName, final String requesterPrincipal) {
        return switch (tagName) {
            case DifcTags.USER_CONTACT ->
                    DifcTags.PRINCIPAL_NOTIFICATION_SERVICE.equals(requesterPrincipal);
            case DifcTags.USER_SHIPPING ->
                    DifcTags.PRINCIPAL_USER_ADDRESS_SERVICE.equals(requesterPrincipal);
            default -> false;
        };
    }
}
