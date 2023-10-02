package com.sclab.queue.config;

public interface SubscriberConfig extends PubSubConfig {
    String POLL_TIMEOUT_MILLIS = "poll-timeout-millis";
    String CONSUME_UNTIL_SECONDS = "consume-until-seconds";
    String CONSUME_UNTIL_MINUTES = "consume-until-minutes";
    String KEY_DESERIALIZER = "key.deserializer";
    String VALUE_DESERIALIZER = "value.deserializer";
}