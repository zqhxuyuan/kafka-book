package kafka.eos;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.ProducerFencedException;

import java.util.Properties;

/**
 * Created by zhengqh on 17/7/25.
 */
public class KafkaEosTest {

    public static void main(String[] args) {
        String topic = "test-eos";

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("client.id", "DemoProducer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        props.put("transactional.id", "kafka-eos-00001");
        KafkaProducer<Integer, String> producer = new KafkaProducer<>(props);

        ProducerRecord<Integer, String> record1 = new ProducerRecord<Integer, String>(topic, 1, "one");
        ProducerRecord<Integer, String> record2 = new ProducerRecord<Integer, String>(topic, 2, "two");

        producer.initTransactions();
        try {
            producer.beginTransaction();
            producer.send(record1);
            producer.send(record2);
            producer.commitTransaction();
        } catch(ProducerFencedException e) {
            producer.close();
        } catch(KafkaException e) {
            producer.abortTransaction();
        }
    }
}
