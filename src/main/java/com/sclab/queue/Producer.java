package com.sclab.queue;

import com.sclab.queue.config.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;

public class Producer {

    public static void main(String[] args) {
        String testData = "";
        publishMessage(testData);
    }

    public static void publishMessage(String testData){
        System.out.println("publishing message ...");
    }

    private static void publishMessage(
            Properties properties,
            String topic,
            String key,
            String value
    ){
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);
        publishMessage(properties, producerRecord);
    }

    private static void publishMessage(
            Properties properties,
            String key,
            String value
    ){
        String topic = properties.getProperty(ConsumerConfig.TOPIC);
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);
        publishMessage(properties, producerRecord);
    }

    private static void publishMessage(
            Properties properties,
            String value
    ){
        String topic = properties.getProperty(ConsumerConfig.TOPIC);
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, value);
        publishMessage(properties, producerRecord);
    }

    /**
     * Main method to publish message
     * @param properties
     * @param producerRecord
     */
    public static void publishMessage(Properties properties, ProducerRecord<String, String> producerRecord) {
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        kafkaProducer.send(producerRecord);
    }
}