package com.sclab.queue;

import com.sclab.queue.config.PublisherConfig;
import com.sclab.queue.config.SubscriberConfig;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;

public class ProducerImpl {

    public static void main(String[] args) {
        String testData = "";
        publishMessage(testData);
    }

    public static void publishMessage(String testData) {
        System.out.println("publishing message ...");
    }

    private static void publishMessage(
            Properties properties,
            String topic,
            String key,
            String value
    ) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);
        publishMessage(properties, producerRecord);
    }

    private static void publishMessage(
            Properties properties,
            String key,
            String value
    ) {
        String topic = properties.getProperty(PublisherConfig.TOPIC);
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);
        publishMessage(properties, producerRecord);
    }

    private static void publishMessage(
            Properties properties,
            String value
    ) {
        String topic = properties.getProperty(SubscriberConfig.TOPIC);
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, value);
        publishMessage(properties, producerRecord);
    }

    /**
     * Main method to publish message
     *
     * @param properties
     * @param producerRecord
     */
    public static void publishMessage(Properties properties, ProducerRecord<String, String> producerRecord) {
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        kafkaProducer.send(producerRecord);
    }

    /**
     *
     * @param topicName
     * @param key
     * @param avroSchemaData: convert avro json schema into Java Classes, set values and pass here
     * @param properties
     */
    public static void sendMessageWithSpecificKafkaSchema(
            String topicName,
            String key,
            SpecificRecord avroSchemaData,
            Properties properties
    ) {
        // ** We can use GenericRecord or SpecificRecord
        System.out.println("making producer record...");
        ProducerRecord<String, SpecificRecord> record = new ProducerRecord<>(topicName, key, avroSchemaData);
        System.out.println("making producer...");
        Producer<String, SpecificRecord> producer = new KafkaProducer<>(properties);
        System.out.println("sending data...");
        producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception == null) {
                            System.out.println("Record sent successfully to partition " + metadata.partition()
                                    + " at offset " + metadata.offset());
                        } else {
                            System.err.println("Error sending record: " + exception.getMessage());
                        }
                    }
                }
        );
        System.out.println("flushing producer...");
        producer.flush();
        System.out.println("closing producer....");
        producer.close();
        System.out.println("all process has been completed.");
    }
}