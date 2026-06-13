package com.springbootkafka.difc;

import org.apache.kafka.common.message.PollPrivsReqResponseData;

/**
 * Invoked when {@code POLL_PRIVS_REQ} returns a pending capability request for a tag owner.
 * Mirrors {@code org.apache.kafka.streams.difc.DifcPrivilegeRequestHandler}.
 */
@FunctionalInterface
public interface DifcPrivilegeRequestHandler {

    /**
     * @param pendingRequest {@code capability} is {@code -1} when the queue is empty (ignore).
     */
    void onPrivilegeRequest(PollPrivsReqResponseData pendingRequest);
}
