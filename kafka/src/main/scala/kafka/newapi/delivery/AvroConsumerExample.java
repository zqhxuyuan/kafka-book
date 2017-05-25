package kafka.newapi.delivery;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Properties;


/**
 * Reads an avro message.
 */
public class AvroConsumerExample {

    public static void main(String[] str) throws InterruptedException {
        System.out.println("Starting AutoOffsetAvroConsumerExample ...");
        readMessages();
    }

    private static void readMessages() throws InterruptedException {
        KafkaConsumer<String, byte[]> consumer = createConsumer();
        // Assign to specific topic and partition, subscribe could be used here to subscribe to all topic.
        consumer.assign(Arrays.asList(new TopicPartition("avro-topic", 0)));
        processRecords(consumer);
    }

    private static void processRecords(KafkaConsumer<String, byte[]> consumer) throws InterruptedException {
        while (true) {
            ConsumerRecords<String, byte[]> records = consumer.poll(100);
            long lastOffset = 0;
            for (ConsumerRecord<String, byte[]> record : records) {
                GenericRecord genericRecord = AvroSupport.byteArrayToData(AvroSupport.getSchema(), record.value());
                String firstName = AvroSupport.getValue(genericRecord, "firstName", String.class);
                System.out.printf("\n\roffset = %d, key = %s, value = %s", record.offset(), record.key(), firstName);
                lastOffset = record.offset();
            }
            System.out.println("lastOffset read: " + lastOffset);
            consumer.commitSync();
            Thread.sleep(500);
        }
    }

    private static KafkaConsumer<String, byte[]> createConsumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "cg1");
        props.put("enable.auto.commit", "true");
        props.put("auto.offset.reset", "earliest");
        props.put("auto.commit.interval.ms", "100");
        props.put("heartbeat.interval.ms", "3000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        return new KafkaConsumer<String, byte[]>(props);
    }
}
