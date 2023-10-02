package com.sclab.queue.config;

public interface PublisherConfig extends PubSubConfig {
    String KEY_SERIALIZER = "key.serializer";
    String VALUE_SERIALIZER = "value.serializer";
}