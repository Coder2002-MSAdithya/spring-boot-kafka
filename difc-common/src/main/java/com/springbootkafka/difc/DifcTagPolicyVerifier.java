package com.springbootkafka.difc;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Grantor-side policy loading for the {@code spring-boot-kafka} workflow.
 *
 * <p>Only {@code CAN_ADD} external-connection verification is used here (no {@code CAN_REMOVE}).
 */
public final class DifcTagPolicyVerifier {

    public static final String POLICY_REGISTRY_DIR_PROP = "policy.registry.dir";

    private DifcTagPolicyVerifier() {
    }

    public static Path policyPathForRequester(final String requesterPrincipal) {
        final String registryDir =
                System.getProperty(POLICY_REGISTRY_DIR_PROP, "/tmp/sbk-kafka-demo/policy");
        return Paths.get(registryDir, requesterPrincipal, "processing-policy.json");
    }

    public static AppProcessingPolicy loadRequesterPolicy(final String requesterPrincipal)
            throws IOException {
        final PolicyAttestationVerifier.LoadedAttestedPolicy loaded =
                PolicyAttestationVerifier.loadAndVerifyAttestation(requesterPrincipal);
        if (!loaded.attestationResult().allowed()) {
            throw new IOException(loaded.attestationResult().reason());
        }
        return loaded.policy();
    }

    public static AppProcessingPolicy loadRequesterPolicyFromBytes(
            final String requesterPrincipal,
            final byte[] policyBytes) throws IOException {
        final PolicyAttestationVerifier.LoadedAttestedPolicy loaded =
                PolicyAttestationVerifier.loadAndVerifyAttestationFromBytes(requesterPrincipal, policyBytes);
        if (!loaded.attestationResult().allowed()) {
            throw new IOException(loaded.attestationResult().reason());
        }
        return loaded.policy();
    }

    public record VerificationResult(boolean allowed, String reason) {
        public static VerificationResult allow() {
            return new VerificationResult(true, "policy verified");
        }

        public static VerificationResult allow(final String reason) {
            return new VerificationResult(true, reason);
        }

        public static VerificationResult deny(final String reason) {
            return new VerificationResult(false, reason);
        }
    }
}
