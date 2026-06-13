package com.notificationconsumer.repository;

import com.notificationconsumer.entity.Notification;
import org.springframework.context.annotation.Profile;
import org.springframework.data.couchbase.repository.CouchbaseRepository;
import org.springframework.stereotype.Repository;

@Repository
@Profile("!workflow")
public interface NotificationRepository extends CouchbaseRepository<Notification, Long> {

}
