package com.springbootkafka.difc;

import org.apache.kafka.clients.Capability;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.message.AddClientPrivsResponseData;
import org.apache.kafka.common.message.GrantCapResponseData;
import org.apache.kafka.common.message.PollPrivsReqResponseData;
import org.slf4j.Logger;

/**
 * DIFC {@code GRANT_CAP} / {@code POLL_PRIVS_REQ} helpers aligned with
 * {@code io.confluent.examples.streams.microservices.util.MicroserviceUtils}.
 */
public final class DifcPrivilegeRequests {

    private DifcPrivilegeRequests() {
    }

    public static Capability capabilityFromPollResponse(final byte capability) {
        return switch (capability) {
            case 0 -> Capability.CAN_ADD;
            case 1 -> Capability.CAN_REMOVE;
            default -> throw new IllegalArgumentException("Unknown DIFC capability code: " + capability);
        };
    }

    /**
     * Poll the tag-owner queue and dispatch to the handler (streams owner services do this on a background thread).
     */
    public static void pollAndDispatch(
            final Logger log,
            final String serviceName,
            final KafkaProducer<?, ?> producer,
            final DifcPrivilegeRequestHandler handler) {
        final PollPrivsReqResponseData pending = producer.pollPrivsReq();
        if (pending == null) {
            return;
        }
        if (pending.capability() >= 0
                && pending.tagName() != null
                && !pending.tagName().isEmpty()) {
            log.info(
                    "POLL_PRIVS_REQ pending request: tag={}, capability={}, requester={}",
                    pending.tagName(),
                    pending.capability(),
                    pending.requesterClientId());
        } else if (log.isTraceEnabled()) {
            log.trace("POLL_PRIVS_REQ queue empty (capability={})", pending.capability());
        }
        handler.onPrivilegeRequest(pending);
    }

    /**
     * Tag owner approves a queued request via {@code ADD_CLIENT_PRIVS}.
     */
    public static void grantPendingPrivilegeRequest(
            final Logger log,
            final String serviceName,
            final KafkaProducer<?, ?> producer,
            final PollPrivsReqResponseData pending) {
        if (pending == null || pending.capability() < 0) {
            return;
        }
        final String tagName = pending.tagName();
        final String requester = pending.requesterClientId();
        if (tagName == null || tagName.isEmpty() || requester == null || requester.isEmpty()) {
            return;
        }
        final Capability capability = capabilityFromPollResponse(pending.capability());
        if (!DifcGrantPolicy.isAllowedGrant(tagName, requester, capability)) {
            final String denyLine = String.format(
                    "[DIFC] grantDenied service=%s requester=%s tag=%s capability=%s%n",
                    serviceName,
                    requester,
                    tagName,
                    capability);
            log.warn(denyLine.trim());
            System.out.print(denyLine);
            return;
        }
        System.out.printf(
                "[DIFC] grantLineageVerify workflow=spring-boot-kafka service=%s requester=%s tag=%s "
                    + "method=allow-list-only note=expression-lineage-not-used-for-CAN_ADD%n",
                serviceName,
                requester,
                tagName);
        final AddClientPrivsResponseData response =
                producer.addClientPrivs(requester, tagName, capability);
        final String line = String.format(
                "[DIFC] grantPrivilege service=%s requester=%s tag=%s capability=%s errorCode=%d message=%s%n",
                serviceName,
                requester,
                tagName,
                capability,
                response.errorCode(),
                response.errorMessage());
        if (response.errorCode() == 0) {
            log.info(line.trim());
        } else {
            log.warn(line.trim());
        }
        System.out.print(line);
    }

    /**
     * Grant queued requests only when {@link DifcGrantPolicy} allows the requester/tag/capability triple.
     */
    public static DifcPrivilegeRequestHandler autoGrantPrivilegeRequests(
            final Logger log,
            final String serviceName,
            final KafkaProducer<?, ?> producer) {
        return pending -> grantPendingPrivilegeRequest(log, serviceName, producer, pending);
    }

    public static void requestGrantCap(
            final Logger log,
            final String serviceName,
            final KafkaConsumer<?, ?> consumer,
            final String tagName,
            final Capability capability) {
        final GrantCapResponseData response = capability == Capability.CAN_ADD
                ? consumer.requestAddCapabilityForTag(tagName)
                : consumer.requestRemoveCapabilityForTag(tagName);
        final String line = String.format(
                "[DIFC] requestGrantCap service=%s tag=%s capability=%s errorCode=%d message=%s%n",
                serviceName,
                tagName,
                capability,
                response.errorCode(),
                response.errorMessage());
        log.info(line.trim());
        System.out.print(line);
        DifcOps.requireOk(
                response.errorCode(),
                "requestGrantCap(" + serviceName + ", " + tagName + ", " + capability + ")",
                response.errorMessage());
    }

    /** Enqueue {@code CAN_ADD} on a tag owned by another client (consume-only services). */
    public static void requestGrantCapOnly(
            final Logger log,
            final String serviceName,
            final KafkaConsumer<?, ?> consumer,
            final String tagName) {
        requestGrantCap(log, serviceName, consumer, tagName, Capability.CAN_ADD);
    }

    /**
     * Request {@code CAN_ADD} so the shared consumer can read tagged records ({@code addTag} on label).
     * Use when the service does not publish with {@code declassifyTags}.
     */
    public static void requestGrantCapForConsume(
            final Logger log,
            final String serviceName,
            final KafkaConsumer<?, ?> consumer,
            final String tagName) {
        requestGrantCapOnly(log, serviceName, consumer, tagName);
    }

    /**
     * Request {@code CAN_ADD} and {@code CAN_REMOVE}, then rely on the startup gate to {@code addTag}.
     * Use when the service consumes a tagged slice and later declassifies that tag on produce.
     */
    public static void requestGrantCapAddAndRemove(
            final Logger log,
            final String serviceName,
            final KafkaConsumer<?, ?> consumer,
            final String tagName) {
        requestGrantCap(log, serviceName, consumer, tagName, Capability.CAN_ADD);
        requestGrantCap(log, serviceName, consumer, tagName, Capability.CAN_REMOVE);
    }

    /**
     * Apply label tags on the shared consumer so FETCH redaction allows reading tagged records.
     *
     * @return {@code true} when every tag was added successfully (grants may still be pending).
     */
    public static boolean tryApplyConsumerLabels(
            final Logger log,
            final String serviceName,
            final KafkaConsumer<?, ?> consumer,
            final String... labelTags) {
        boolean ok = true;
        for (final String tag : labelTags) {
            final var label = consumer.addTag(tag);
            if (label.errorCode() != 0) {
                ok = false;
                final String line = String.format(
                        "[DIFC] addTag label service=%s tag=%s errorCode=%d message=%s%n",
                        serviceName,
                        tag,
                        label.errorCode(),
                        label.errorMessage());
                log.warn(line.trim());
                System.out.print(line);
            }
        }
        return ok;
    }
}
