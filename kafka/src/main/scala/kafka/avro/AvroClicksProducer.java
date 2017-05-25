package kafka.avro;

import avro.LogLine;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

public class AvroClicksProducer {

    public static void main(String[] args) {
        Properties props = new Properties();
        // hardcoding the Kafka server URI for this example
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("schema.registry.url", "http://localhost:8081");
        // Hard coding topic too.
        String topic = "clicks";

        Producer<String, LogLine> producer = new KafkaProducer<String, LogLine>(props);

        Random rnd = new Random();
        for (long nEvents = 0; nEvents < 100; nEvents++) {
            LogLine event = EventGenerator.getNext();

            // Using IP as key, so events from same IP will go to same partition
            ProducerRecord<String, LogLine> record = new ProducerRecord<String, LogLine>(topic, event.getIp().toString(), event);
            producer.send(record);
        }

    }
}