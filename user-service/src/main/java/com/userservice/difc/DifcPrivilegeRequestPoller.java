package com.userservice.difc;

import com.springbootkafka.difc.DifcPrivilegeRequestHandler;
import com.springbootkafka.difc.DifcPrivilegeRequests;
import com.springbootkafka.difc.DifcTags;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Tag owner polls {@code POLL_PRIVS_REQ} and grants queued {@code GRANT_CAP} requests when
 * {@link com.springbootkafka.difc.DifcGrantPolicy} allows the requester/tag/capability triple.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class DifcPrivilegeRequestPoller {

    private final KafkaProducer<String, String> difcKafkaProducer;
    private DifcPrivilegeRequestHandler grantHandler;

    @jakarta.annotation.PostConstruct
    void initGrantHandler() {
        grantHandler = DifcPrivilegeRequests.autoGrantPrivilegeRequests(
                log,
                DifcTags.PRINCIPAL_USER_SERVICE,
                difcKafkaProducer);
    }

    @Scheduled(fixedDelay = 1000, initialDelay = 3000)
    public void pollPendingPrivilegeRequests() {
        DifcPrivilegeRequests.pollAndDispatch(
                log,
                DifcTags.PRINCIPAL_USER_SERVICE,
                difcKafkaProducer,
                grantHandler);
    }
}
