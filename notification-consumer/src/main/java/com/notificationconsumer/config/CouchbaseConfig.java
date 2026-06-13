package com.notificationconsumer.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.couchbase.config.AbstractCouchbaseConfiguration;

@Configuration
@ConditionalOnProperty(name = "couchbase.enabled", havingValue = "true", matchIfMissing = true)
@ConfigurationProperties(prefix = "couchbase.notification-service.config")
@Getter
@Setter
public class CouchbaseConfig extends AbstractCouchbaseConfiguration {

    private String bootstrapHosts;
    private String notificationBucketUsername;
    private String notificationPassword;
    private String notificationBucket;

    @Override
    public String getConnectionString() {
        return bootstrapHosts;
    }

    @Override
    public String getUserName() {
        return notificationBucketUsername;
    }

    @Override
    public String getPassword() {
        return notificationPassword;
    }

    @Override
    public String getBucketName() {
        return notificationBucket;
    }
}
