package com.springbootkafka.difc;

import org.apache.kafka.clients.Capability;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.message.AddClientPrivsResponseData;
import org.apache.kafka.common.message.AddTagResponseData;
import org.apache.kafka.common.message.CreateTagResponseData;
import org.apache.kafka.common.message.RegisterClientResponseData;
import org.slf4j.Logger;

public final class DifcOps {

    private DifcOps() {
    }

    public static void requireOk(final int errorCode, final String operation, final String detail) {
        if (errorCode != 0) {
            throw new IllegalStateException(
                    operation + " failed with errorCode=" + errorCode
                            + (detail == null || detail.isEmpty() ? "" : ": " + detail));
        }
    }

    public static void logRegisterClient(final Logger log, final String service, final RegisterClientResponseData response) {
        final String line = String.format(
                "[DIFC] registerClient service=%s errorCode=%d message=%s%n",
                service, response.errorCode(), response.errorMessage());
        log.info(line.trim());
        System.out.print(line);
    }

    public static void logCreateTag(final Logger log, final String service, final String tag, final CreateTagResponseData response) {
        final String line = String.format(
                "[DIFC] createTag service=%s tag=%s errorCode=%d message=%s%n",
                service, tag, response.errorCode(), response.errorMessage());
        log.info(line.trim());
        System.out.print(line);
    }

    public static void logAddTag(final Logger log, final String service, final String tag, final AddTagResponseData response) {
        final String line = String.format(
                "[DIFC] addTag service=%s tag=%s errorCode=%d message=%s%n",
                service, tag, response.errorCode(), response.errorMessage());
        log.info(line.trim());
        System.out.print(line);
    }

    public static void logAddClientPrivs(
            final Logger log,
            final String owner,
            final String target,
            final String tag,
            final Capability capability,
            final AddClientPrivsResponseData response) {
        final String line = String.format(
                "[DIFC] addClientPrivs owner=%s target=%s tag=%s capability=%s errorCode=%d message=%s%n",
                owner, target, tag, capability, response.errorCode(), response.errorMessage());
        if (response.errorCode() == 0) {
            log.info(line.trim());
        } else {
            log.warn(line.trim());
        }
        System.out.print(line);
    }

    public static void bootstrapProducerTags(
            final Logger log,
            final String serviceName,
            final KafkaProducer<?, ?> producer) {
        final RegisterClientResponseData register = producer.registerClient();
        logRegisterClient(log, serviceName, register);
        requireOk(register.errorCode(), "registerClient(" + serviceName + ")", register.errorMessage());

        for (final String tag : new String[]{DifcTags.USER_CONTACT, DifcTags.USER_SHIPPING}) {
            final CreateTagResponseData created = producer.createTag(tag);
            logCreateTag(log, serviceName, tag, created);
            requireOk(created.errorCode(), "createTag(" + tag + ")", created.errorMessage());
        }
        // Do not addTag on the producer: broker merges sender tags with record tags, which would
        // attach every owned tag to every message. Per-record sendWithTags supplies the label.
    }

    public static void registerConsumer(final Logger log, final String serviceName, final KafkaConsumer<?, ?> consumer) {
        final RegisterClientResponseData register = consumer.registerClient();
        logRegisterClient(log, serviceName, register);
        if (register.errorCode() == 133) {
            return;
        }
        requireOk(register.errorCode(), "registerClient(" + serviceName + ")", register.errorMessage());
    }

    public static boolean refreshConsumerLabel(
            final Logger log,
            final String serviceName,
            final Consumer<?, ?> consumer,
            final String... labelTags) {
        if (!(consumer instanceof KafkaConsumer<?, ?> kafkaConsumer)) {
            return false;
        }
        boolean ok = true;
        for (final String tag : labelTags) {
            final AddTagResponseData label = kafkaConsumer.addTag(tag);
            if (label.errorCode() != 0) {
                ok = false;
                final String line = String.format(
                        "[DIFC] addTag refresh service=%s tag=%s errorCode=%d message=%s%n",
                        serviceName, tag, label.errorCode(), label.errorMessage());
                log.warn(line.trim());
                System.out.print(line);
            }
        }
        return ok;
    }
}
