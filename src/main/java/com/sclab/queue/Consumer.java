package com.sclab.queue;

import com.sclab.queue.util.FileUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Consumer {
    final static String PROPERTY_FILE_PATH = "src/main/resources/application.properties";
    final static String OUT_DATA_FILE_PATH = "src/main/resources/data.csv";
    final static String TOPIC = "topic";
    final static String POLL_TIMEOUT_MILLIS = "poll-timeout-millis";

    public static void main(String[] args) {
        consumeMessages();
    }

    public static void consumeMessages() {
        // create blank file to store out data
        FileUtil.createNewFile(OUT_DATA_FILE_PATH);
        FileUtil.appendIntoFile(OUT_DATA_FILE_PATH, "key,value,headers\n");
        // Create Properties and put sensitive data
        Properties properties = FileUtil.readPropertyFile(PROPERTY_FILE_PATH);
        // create consumer
        final KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        // subscribe to topic
        kafkaConsumer.subscribe(Arrays.asList(properties.getProperty(TOPIC)));
        // poll and consume records
        while (true){
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(
                    Duration.ofMillis(Long.parseLong(properties.getProperty(POLL_TIMEOUT_MILLIS))));
            for(ConsumerRecord<String, String> consumerRecord: consumerRecords){
                FileUtil.appendIntoFile(OUT_DATA_FILE_PATH,
                        consumerRecord.key()+","+consumerRecord.value()+","+consumerRecord.headers()+"\n");
            }
        }
    }
}