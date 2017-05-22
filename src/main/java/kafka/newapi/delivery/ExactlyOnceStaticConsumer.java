package kafka.newapi.delivery;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;


/**
 * To achieve exactly once scenario, offset should be manually managed (DO-NOT do auto offset management).
 * <p/>
 * The consumer can register with the Kafka broker with either (a) Auto load balance feature, this is
 * the scenario where if other consumers joins or fails. Kafka automatically re-balance the topic/partition
 * load with available consumers at that point. Or (b) Consumer register for a specific topic/partition, so no
 * automatic re-balance offered by Kafka at that point. Both these options (a & b) can be used in auto
 * or manual offset management.
 * <p/>
 * This example while demonstrates exactly once scenario, it also demonstrate above specific topic
 * partition registering option which is option (b) from the above. Please note that registering
 * for specific/static topic/partition is not mandatory to showcase exactly once scenario.
 * <p/>
 * Following are the steps for exactly once scenario by doing manual offset management and enabling
 * specific topic/partition registration.
 * <p/>
 * 1) enable.auto.commit = false
 * 2) Don't make call to consumer.commitSync(); after processing record.
 * 3) Register consumer to specific partition using 'assign' call.
 * 4) On start up of the consumer seek by consumer.seek(topicPartition,offset); to the offset where
 * you want to start reading from.
 * 5) Process records and get hold of the offset of each record, and store offset in an atomic way
 * along with the processed data using atomic-transaction to get the exactly-once semantic. (with data
 * in relational database this is easier) for non-relational  such as HDFS store the offset where the
 * data is since they don't support atomic transactions.
 * 6) Implement idempotent as a safety net.
 * <p/>
 */
public class ExactlyOnceStaticConsumer {

    private static OffsetManager offsetManager = new OffsetManager("storage1");


    public static void main(String[] str) throws InterruptedException, IOException {

        System.out.println("Starting ManualOffsetGuaranteedExactlyOnceReadingFromSpecificPartitionConsumer ...");

        readMessages();


    }


    private static void readMessages() throws InterruptedException, IOException {

        KafkaConsumer<String, String> consumer = createConsumer();

        String topic = "normal-topic";
        int partition = 1;

        TopicPartition topicPartition = registerConsumerToSpecificPartition(consumer, topic, partition);

        // Read the offset for the topic and partition from external storage.
        long offset = offsetManager.readOffsetFromExternalStore(topic, partition);

        // Use seek and go to exact offset for that topic and partition.
        consumer.seek(topicPartition, offset);

        processRecords(consumer);

    }


    private static KafkaConsumer<String, String> createConsumer() {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        String consumeGroup = "cg2";

        props.put("group.id", consumeGroup);

        // Below is a key setting to turn off the auto commit.
        props.put("enable.auto.commit", "false");
        props.put("heartbeat.interval.ms", "2000");
        props.put("session.timeout.ms", "6001");


        // control maximum data on each poll, make sure this value is bigger than the maximum single record size
        props.put("max.partition.fetch.bytes", "40");

        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return new KafkaConsumer<String, String>(props);
    }


    /**
     * Manually listens for specific topic partition. But, if you are looking for example of how to dynamically listens
     * to partition and want to manually control offset then see ManualOffsetConsumerWithRebalanceExample.java
     */
    private static TopicPartition registerConsumerToSpecificPartition(KafkaConsumer<String, String> consumer, String topic, int partition) {

        TopicPartition topicPartition = new TopicPartition(topic, partition);
        List<TopicPartition> partitions = Arrays.asList(topicPartition);
        consumer.assign(partitions);
        return topicPartition;

    }


    /**
     * Process data and store offset in external store. Best practice is to do these operations atomically. Read class level comments.
     */
    private static void processRecords(KafkaConsumer<String, String> consumer) throws IOException {
        while (true) {

            ConsumerRecords<String, String> records = consumer.poll(100);

            for (ConsumerRecord<String, String> record : records) {

                System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
                offsetManager.saveOffsetInExternalStore(record.topic(), record.partition(), record.offset());

            }
        }
    }


}
