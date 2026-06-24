package com.springbootkafka.difc;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Grantor-side registry of non-Kafka external endpoints permitted for each
 * {@code spring-boot-kafka} SCRAM principal.
 */
public final class ExpectedExternalConnectionsRegistry {

    private static final Map<String, Set<String>> DEFAULTS;

    static {
        final Map<String, Set<String>> defaults = new LinkedHashMap<>();
        defaults.put(DifcTags.PRINCIPAL_USER_SERVICE, endpoints(
                "localhost:5432", "127.0.0.1:5432"));
        defaults.put(DifcTags.PRINCIPAL_USER_ADDRESS_SERVICE, endpoints(
                "localhost:5432", "127.0.0.1:5432"));
        defaults.put(DifcTags.PRINCIPAL_NOTIFICATION_SERVICE, endpoints(
                "localhost:8091", "127.0.0.1:8091",
                "localhost:8092", "127.0.0.1:8092",
                "localhost:8093", "127.0.0.1:8093",
                "localhost:8094", "127.0.0.1:8094",
                "localhost:8095", "127.0.0.1:8095",
                "localhost:8096", "127.0.0.1:8096",
                "localhost:11210", "127.0.0.1:11210"));
        DEFAULTS = Collections.unmodifiableMap(defaults);
    }

    private ExpectedExternalConnectionsRegistry() {
    }

    public static Set<String> defaultsForPrincipal(final String principal) {
        if (principal == null) {
            return Set.of();
        }
        return DEFAULTS.getOrDefault(normalizePrincipal(principal), Set.of());
    }

    private static Set<String> endpoints(final String... values) {
        final Set<String> set = new LinkedHashSet<>();
        Collections.addAll(set, values);
        return Collections.unmodifiableSet(set);
    }

    private static String normalizePrincipal(final String principal) {
        if (principal.endsWith("-consumer")) {
            return principal.substring(0, principal.length() - "-consumer".length());
        }
        if (principal.endsWith("-producer")) {
            return principal.substring(0, principal.length() - "-producer".length());
        }
        return principal;
    }
}
