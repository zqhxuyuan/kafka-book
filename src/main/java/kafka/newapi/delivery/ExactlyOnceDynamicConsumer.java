package kafka.newapi.delivery;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * To achieve exactly once scenario, offset should be manually managed (DO-NOT do auto offset management).
 * <p/>
 * The consumer can register with the Kafka broker with either (a) Auto load balance feature, this is
 * the scenario where if other consumers joins or fails. Kafka automatically re-balance the topic/partition
 * load with available consumers at that point. Or (b) Consumer register for a specific topic/partition,
 * so no automatic re-balance offered by Kafka at that point. Both these options (a & b) can be used
 * in auto or manual offset management.
 * <p/>
 * This example while demonstrates exactly once scenario, it also demonstrate above auto re-balance
 * option which is option (a) feature from the above. Please note that registering for auto re-balance
 * to topic/partition is not mandatory to showcase exactly once scenario.
 * <p/>
 * Following are the steps for exactly once scenario by doing manual offset management and enabling
 * automatic re-balance.
 * <p/>
 * 1) enable.auto.commit = false
 * 2) Don't make call to consumer.commitSync(); after processing record.
 * 3) Register consumer to topics to get dynamically assigned partitions using 'subscribe' call.
 * 4) Implement a ConsumerRebalanceListener and seek by calling consumer.seek(topicPartition,offset);
 * to start reading from for a specific partition.
 * 5) Process records and get hold of the offset of each record, and store offset in an atomic way along
 * with the processed data  using atomic-transaction to get the exactly-once semantic. (with data in
 * relational database this is easier) for non-relational  such as HDFS store the offset where the data
 * is since they don't support atomic transactions.
 * 6) Implement idempotent as a safety net.
 * <p/>
 */
public class ExactlyOnceDynamicConsumer {

    private static OffsetManager offsetManager = new OffsetManager("storage2");

    public static void main(String[] str) throws InterruptedException {

        System.out.println("Starting ManualOffsetGuaranteedExactlyOnceReadingDynamicallyBalancedPartitionConsumer ...");

        readMessages();

    }

    /**
     */
    private static void readMessages() throws InterruptedException {

        KafkaConsumer<String, String> consumer = createConsumer();

        // Manually controlling offset but register consumer to topics to get dynamically assigned partitions.
        // Inside MyConsumerRebalancerListener use consumer.seek(topicPartition,offset) to control offset

        consumer.subscribe(Arrays.asList("normal-topic"), new MyConsumerRebalancerListener(consumer));

        processRecords(consumer);
    }

    private static KafkaConsumer<String, String> createConsumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        String consumeGroup = "cg3";

        props.put("group.id", consumeGroup);

        // Below is a key setting to turn off the auto commit.
        props.put("enable.auto.commit", "false");
        props.put("heartbeat.interval.ms", "2000");
        props.put("session.timeout.ms", "6001");

        // Control maximum data on each poll, make sure this value is bigger than the maximum single record size
        props.put("max.partition.fetch.bytes", "40");

        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return new KafkaConsumer<String, String>(props);
    }

    private static void processRecords(KafkaConsumer<String, String> consumer) {

        while (true) {

            ConsumerRecords<String, String> records = consumer.poll(100);

            for (ConsumerRecord<String, String> record : records) {

                System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
                offsetManager.saveOffsetInExternalStore(record.topic(), record.partition(), record.offset());

            }
        }
    }


}
