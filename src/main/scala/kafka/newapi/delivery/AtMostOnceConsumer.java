package kafka.newapi.delivery;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;


/**
 * At-most-once is the default behavior of KAFKA, Depending on how consumer is configured for auto
 * offset management, in some cases this message pattern would turn into at-least-once rather than at-most-once.
 * <p/>
 * Since at-most-once is the lower messaging guarantee we would declare this consumer as at-most-once.
 * <p/>
 * To get to this behavior ‘enable.auto.commit’ to true and set ‘auto.commit.interval.ms’ to a lower time-frame.
 * And do not make call to consumer.commitSync(); from the consumer. Now Kafka would auto commit offset at
 * the specified interval.
 * <p/>
 * <p/>
 * Below you would find explanation of when consumer behaves as at-most-once and when it behaves as
 * at-least-once.
 * <p/>
 * To get to this behavior set 'auto.commit.interval.ms' to a lower time-frame. Do not make call to
 * consumer.commitSync(); from the consumer. Now Kafka would auto commit offset at the specified interval.
 * <p/>
 * 'At-most-once' consumer behaviour happens as explained below.
 * <p/>
 * <ol>
 * <li> The commit interval passes and Kafka commits the offset, but client did not complete the
 * processing of the message and client crashes. Now when client restarts it looses the committed message.
 * </li>
 * <p/>
 * 'At-least-once' scenario is below.
 * <p/>
 * <li>Client processed a message and committed to its persistent store. But the Kafka commit interval is NOT
 * passed and kafka could not commit the offset. At this point clients dies, now when client restarts it
 * re-process the same message again.</li>
 * </ol>
 * <p/>
 * </ol>
 */
public class AtMostOnceConsumer {

    public static void main(String[] str) throws InterruptedException {

        System.out.println("Starting AutoOffsetMostlyAtleastOnceButSometimeAtMostOnceConsumer ...");

        execute();

    }

    private static void execute() throws InterruptedException {

        KafkaConsumer<String, String> consumer = createConsumer();

        // Subscribe to all partition in that topic. 'assign' could be used here
        // instead of 'subscribe' to subscribe to specific partition.
        consumer.subscribe(Arrays.asList("normal-topic"));

        processRecords(consumer);
    }

    private static KafkaConsumer<String, String> createConsumer() {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        String consumeGroup = "cg1";
        props.put("group.id", consumeGroup);

        // Set this property, if auto commit should happen.
        props.put("enable.auto.commit", "true");

        // Auto commit interval is an important property, kafka would commit offset at this interval.
        props.put("auto.commit.interval.ms", "101");

        // This is how to control number of records being read in each poll
        props.put("max.partition.fetch.bytes", "35");

        // Set this if you want to always read from beginning.
        //        props.put("auto.offset.reset", "earliest");

        props.put("heartbeat.interval.ms", "3000");
        props.put("session.timeout.ms", "6001");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return new KafkaConsumer<String, String>(props);
    }

    private static void processRecords(KafkaConsumer<String, String> consumer) throws InterruptedException {

        while (true) {

            ConsumerRecords<String, String> records = consumer.poll(100);
            long lastOffset = 0;
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("\n\roffset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
                lastOffset = record.offset();
            }

            System.out.println("lastOffset read: " + lastOffset);

            process();

        }
    }

    private static void process() throws InterruptedException {

        // create some delay to simulate processing of the record.
        Thread.sleep(500);
    }

}
