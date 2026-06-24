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

    /**
     * Principal-only allow-list check (used before attested processing-policy verification).
     */
    public static boolean isPrincipalAllowedGrant(
            final String tagName,
            final String requesterPrincipal,
            final Capability capability) {
        return isAllowedGrant(tagName, requesterPrincipal, capability);
    }

    /**
     * Human-readable explanation for grant denials (included in {@code [DIFC][GRANT-DECISION]} logs).
     */
    public static String denyExplanation(
            final String tagName,
            final String requesterPrincipal,
            final Capability capability) {
        if (capability == Capability.CAN_REMOVE) {
            return requesterPrincipal + " requested CAN_REMOVE on tag '" + tagName
                    + "', but the spring-boot-kafka workflow grants only CAN_ADD (consume-only"
                    + " downstream services never declassify on produce).";
        }
        if (DifcTags.USER_CONTACT.equals(tagName)) {
            return requesterPrincipal + " is not in the CAN_ADD allow-list for tag 'user-contact'."
                    + " Only notification-svc may read contact PII (name, email).";
        }
        if (DifcTags.USER_SHIPPING.equals(tagName)) {
            return requesterPrincipal + " is not in the CAN_ADD allow-list for tag 'user-shipping'."
                    + " Only user-address-svc may read shipping address fields.";
        }
        return requesterPrincipal + " is not in the CAN_ADD allow-list for tag '" + tagName + "'.";
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
