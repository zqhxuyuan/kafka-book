package kafka.eos;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Created by zhengqh on 17/7/25.
 */
public class KafkaIdempotenceTest {

    public static void main(String[] args) {
        String topic = "test-eos";

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("client.id", "DemoProducer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        props.put("enable.idempotence", true);
        props.put("max.inflight.requests.per.connection", 1);
        props.put("retries", Integer.MAX_VALUE);

        KafkaProducer<Integer, String> producer = new KafkaProducer<>(props);

        ProducerRecord<Integer, String> record1 = new ProducerRecord<Integer, String>(topic, 1, "one");
        producer.send(record1);

    }
}
