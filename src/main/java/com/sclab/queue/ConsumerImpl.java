package com.sclab.queue;

import com.sclab.queue.config.SubscriberConfig;
import com.sclab.queue.util.FileUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerImpl {
    final static String BY_DEFAULT_PROPERTY_FILE_PATH = "src/main/resources/application.properties";
    final static String OUT_DATA_FILE_PATH = "src/main/resources/data.csv";
    static KafkaConsumer<String, String> kafkaConsumer = null;
    static Long consumeUntilSeconds = null;

    public static void main(String[] args) {
        consumeMessages();
    }

    /**
     * Default file should be present in appropriate location.
     */
    public static void consumeMessages() {
        Properties properties = FileUtil.readPropertyFile(BY_DEFAULT_PROPERTY_FILE_PATH);
        consumeMessages(properties);
    }

    /**
     * create blank file to store out data,
     * Create Properties and put sensitive data
     *
     * @param propertyFilePath
     */
    public static void consumeMessages(String propertyFilePath) {
        Properties properties = FileUtil.readPropertyFile(propertyFilePath);
        consumeMessages(properties);
    }

    /**
     * create consumer,
     * subscribe to topic,
     * poll and consume records,
     *
     * @param properties
     */
    public static void consumeMessages(Properties properties) {
        LocalDateTime timeStamp = LocalDateTime.now();
        if(kafkaConsumer==null){
            initConsumer(properties);
        }
        FileUtil.createNewFile(OUT_DATA_FILE_PATH);
        FileUtil.appendIntoFile(OUT_DATA_FILE_PATH, "key,value,headers\n");
        while (true) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(
                    Duration.ofMillis(Long.parseLong(properties.getProperty(SubscriberConfig.POLL_TIMEOUT_MILLIS))));
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                FileUtil.appendIntoFile(OUT_DATA_FILE_PATH,
                        consumerRecord.key() + "," + consumerRecord.value() + "," + consumerRecord.headers() + "\n");
            }
            if (consumeUntilSeconds != null && LocalDateTime.now().isAfter(timeStamp.plusSeconds(consumeUntilSeconds))) {
                break;
            }
        }
    }

    public static void initConsumer(Properties properties){
        if (properties.containsKey(SubscriberConfig.CONSUME_UNTIL_MINUTES)) {
            consumeUntilSeconds =
                    Long.parseLong(properties.getProperty(SubscriberConfig.CONSUME_UNTIL_MINUTES)) * 60;
        }
        if (properties.containsKey(SubscriberConfig.CONSUME_UNTIL_SECONDS)) {
            consumeUntilSeconds =
                    Long.parseLong(properties.getProperty(SubscriberConfig.CONSUME_UNTIL_SECONDS));
        }
        kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Arrays.asList(properties.getProperty(SubscriberConfig.TOPIC)));
    }
}