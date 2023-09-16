package com.sclab.queue;

import com.sclab.queue.config.ConsumerConfig;
import com.sclab.queue.util.FileUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Properties;

public class Consumer {
    final static String BY_DEFAULT_PROPERTY_FILE_PATH = "src/main/resources/application.properties";
    final static String OUT_DATA_FILE_PATH = "src/main/resources/data.csv";

    public static void main(String[] args) {
        consumeMessages();
    }

    public static void consumeMessages() {
        FileUtil.createNewFile(OUT_DATA_FILE_PATH);
        FileUtil.appendIntoFile(OUT_DATA_FILE_PATH, "key,value,headers\n");
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
        FileUtil.createNewFile(OUT_DATA_FILE_PATH);
        FileUtil.appendIntoFile(OUT_DATA_FILE_PATH, "key,value,headers\n");
        Properties properties = FileUtil.readPropertyFile(propertyFilePath);
    }

    /**
     * create consumer,
     * subscribe to topic,
     * poll and consume records,
     *
     * @param properties
     */
    private static void consumeMessages(Properties properties) {
        LocalDateTime timeStamp = LocalDateTime.now();
        Long consumeUntilSeconds = null;
        if (properties.containsKey(ConsumerConfig.CONSUME_UNTIL_MINUTES)) {
            consumeUntilSeconds =
                    Long.parseLong(properties.getProperty(ConsumerConfig.CONSUME_UNTIL_MINUTES)) * 60;
        }
        if (properties.containsKey(ConsumerConfig.CONSUME_UNTIL_SECONDS)) {
            consumeUntilSeconds =
                    Long.parseLong(properties.getProperty(ConsumerConfig.CONSUME_UNTIL_SECONDS));
        }
        final KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Arrays.asList(properties.getProperty(ConsumerConfig.TOPIC)));
        while (true) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(
                    Duration.ofMillis(Long.parseLong(properties.getProperty(ConsumerConfig.POLL_TIMEOUT_MILLIS))));
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                FileUtil.appendIntoFile(OUT_DATA_FILE_PATH,
                        consumerRecord.key() + "," + consumerRecord.value() + "," + consumerRecord.headers() + "\n");
            }
            if (consumeUntilSeconds != null && LocalDateTime.now().isAfter(timeStamp.plusSeconds(consumeUntilSeconds))) {
                break;
            }
        }
    }
}