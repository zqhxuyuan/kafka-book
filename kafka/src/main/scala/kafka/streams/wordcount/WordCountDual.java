package kafka.streams.wordcount;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Arrays;
import java.util.Properties;

/**
 * Created by zhengqh on 17/6/12.
 *
 * KTable output:
 *
 [KTABLE-SOURCE-0000000001]: hello , (2<-null)
 [KTABLE-SOURCE-0000000001]: streams , (1<-null)
 [KTABLE-SOURCE-0000000001]: join , (1<-null)
 [KTABLE-SOURCE-0000000001]: kafka , (3<-null)
 [KTABLE-SOURCE-0000000001]: summit , (1<-null)

 * KStream output
 [KSTREAM-SOURCE-0000000003]: hello , 1
 [KSTREAM-SOURCE-0000000003]: kafka , 1
 [KSTREAM-SOURCE-0000000003]: hello , 2
 [KSTREAM-SOURCE-0000000003]: kafka , 2
 [KSTREAM-SOURCE-0000000003]: streams , 1
 [KSTREAM-SOURCE-0000000003]: join , 1
 [KSTREAM-SOURCE-0000000003]: kafka , 3
 [KSTREAM-SOURCE-0000000003]: summit , 1
 */
public class WordCountDual {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "dsl-wc2");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 15000);

        KStreamBuilder builder = new KStreamBuilder();

        // Stream Table Durability
        KTable<String, Long> table1 = builder.table("ktable-output1", "Counts2");
        table1.print();

        //输出主题已经被另一个源节点注册过了
        //KStream<String, Long> stream = builder.stream("ktable-output1");
        KStream<String, Long> stream = builder.stream("kstream-output1");
        stream.print();

        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();

        Thread.sleep(40000L);

        streams.close();
    }
}
