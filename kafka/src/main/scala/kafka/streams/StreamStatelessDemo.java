package kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.*;

/**
 * Created by zhengqh on 17/6/10.
 *
 * https://kafka.apache.org/0102/javadoc/index.html [this version has sample code]
 * http://docs.confluent.io/current/streams/developer-guide.html#writing-a-kafka-streams-application
 */
public class StreamStatelessDemo {

    static KStreamBuilder builder = new KStreamBuilder();
    KStream<String, Long> wordCountStream = builder.stream(Serdes.String(), Serdes.Long(), "word-counts-input-topic");;
    // Split a sentence into words.
    KStream<byte[], String> sentenceStream = builder.stream(Serdes.ByteArray(), Serdes.String(), "input-topic");

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //...

        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();

        Thread.sleep(5000L);
        streams.close();
    }

    public void streamTypes(){
        KTable<String, Long> ktable =
                builder.table(Serdes.String(), Serdes.Long(), "word-counts-input-topic", "word-counts-partitioned-store");

        GlobalKTable<String, Long> globalKTable =
                builder.globalTable(Serdes.String(), Serdes.Long(), "word-counts-input-topic", "word-counts-global-store");

        wordCountStream = ktable.toStream();
    }

    public void branch() {
        // KStream branches[0] contains all records whose keys start with "A"
        // KStream branches[1] contains all records whose keys start with "B"
        // KStream branches[2] contains all other records
        KStream<String, Long>[] branches = wordCountStream.branch(
                (key, value) -> key.startsWith("A"), /* first predicate  */
                (key, value) -> key.startsWith("B"), /* second predicate */
                (key, value) -> true                 /* third predicate  */
        );
    }

    public void filter() {
        // A filter that selects (keeps) only positive numbers
        // JAVA 8
        KStream<String, Long> onlyPositives = wordCountStream.filter((key, value) -> value > 0);

        // JAVA 7
        onlyPositives = wordCountStream.filter(
                new Predicate<String, Long>() {
                    @Override
                    public boolean test(String key, Long value) {
                        return value > 0;
                    }
                });

        // An inverse filter that discards any negative numbers or zero
        onlyPositives = wordCountStream.filterNot((key, value) -> value <= 0);

        onlyPositives = wordCountStream.filterNot(
                new Predicate<String, Long>() {
                    @Override
                    public boolean test(String key, Long value) {
                        return value <= 0;
                    }
                });
    }

    // side effect
    public void foreach() {
        // Print the contents of the KStream to the local console.
        wordCountStream.foreach((key, value) -> System.out.println(key + " => " + value));

        wordCountStream.foreach(
                new ForeachAction<String, Long>() {
                    @Override
                    public void apply(String key, Long value) {
                        System.out.println(key + " => " + value);
                    }
                });

        wordCountStream.print();
        // 自定义输出类型
        sentenceStream.print(Serdes.ByteArray(), Serdes.String());

        wordCountStream.writeAsText("/path/to/local/output.txt");
        sentenceStream.writeAsText("/path/to/local/output.txt", Serdes.ByteArray(), Serdes.String());

    }

    // through or to
    public void pipe() {
        builder.stream("streams-file-input").to("streams-pipe-output");


    }

    // input record <K,V> can be transformed into an output record <K':V>
    //For example, you can use this transformation to set a key for a key-less input record <null,V> by extracting a key from the value within your KeyValueMapper. The example below computes the new key as the length of the value string.
    public void selectByKey() {
        KStream<Byte[], String> keyLessStream = builder.stream("key-less-topic");
        KStream<Integer, String> keyedStream = keyLessStream.selectKey(new KeyValueMapper<Byte[], String, Integer>(){
            @Override
            public Integer apply(Byte[] key, String value) {
                return value.length();
            }
        });

        // Derive a new record key from the record's value.  Note how the key type changes, too.
        KStream<String, String> rekeyed = sentenceStream.selectKey((key, value) -> value.split(" ")[0]);
        rekeyed = sentenceStream.selectKey(
                new KeyValueMapper<byte[], String, String>() {
                    @Override
                    public String apply(byte[] key, String value) {
                        return value.split(" ")[0];
                    }
                });
    }

    // input record <K,V> can be transformed into an output record <K':V'>
    //The example below normalizes the String key to upper-case letters and counts the number of token of the value string.
    public void map() {
        // String, Long -> Long, String
        KStream<Long, String> countWord = wordCountStream.map(new KeyValueMapper<String, Long, KeyValue<Long, String>>() {
            @Override
            public KeyValue<Long, String> apply(String key, Long value) {
                return new KeyValue<>(value, key);
            }
        });

        countWord = wordCountStream.map((key,value) -> KeyValue.pair(value, key));

        // byte[], String -> String, Integer
        KStream<String, Integer> wordLength = sentenceStream.map(
                (key, value) -> KeyValue.pair(value.toLowerCase(), value.length()));

        wordLength = sentenceStream.map(
                new KeyValueMapper<byte[], String, KeyValue<String, Integer>>() {
                    @Override
                    public KeyValue<String, Integer> apply(byte[] key, String value) {
                        return new KeyValue<>(value.toLowerCase(), value.length());
                    }
                });
    }

    // input record <K,V> can be transformed into an output record <K:V'>
    //The example below counts the number of token of the value string.
    public void mapValues() {
        KStream<String, String> inputStream = builder.stream("topic");
        KStream<String, Integer> outputStream = inputStream.mapValues(new ValueMapper<String, Integer>() {
            @Override
            public Integer apply(String value) {
                return value.split(" ").length;
            }
        });

        KStream<byte[], String> uppercased = sentenceStream.mapValues(value -> value.toUpperCase());

        uppercased = sentenceStream.mapValues(
                new ValueMapper<String, String>() {
                    @Override
                    public String apply(String s) {
                        return s.toUpperCase();
                    }
                });
    }

    //input record <K,V> can be transformed into output records <K':V'>, <K'':V''>
    //The example below splits input records <null:String> containing sentences as values into their words and emit a record <word:1> for each word.
    public void flatMap() {
        KStream<byte[], String> inputStream = builder.stream("topic");
        KStream<String, Integer> outputStream = inputStream.flatMap(new KeyValueMapper<byte[], String, Iterable<KeyValue<String, Integer>>>() {
            @Override
            public Iterable<KeyValue<String, Integer>> apply(byte[] key, String value) {
                String[] tokens = value.split(" ");
                List<KeyValue<String, Integer>> result = new ArrayList<>(tokens.length);

                for(String token : tokens) {
                    result.add(new KeyValue<>(token, 1));
                }
                return result;
            }
        });


        KStream<Long, String> stream1 = builder.stream(Serdes.Long(), Serdes.String(), "topic");
        KStream<String, Integer> flatMapTransformed = stream1.flatMap(
                // Here, we generate two output records for each input record.
                // We also change the key and value types.
                // Example: (345L, "Hello") -> ("HELLO", 1000), ("hello", 9000)
                (key, value) -> {
                    List<KeyValue<String, Integer>> result = new LinkedList<>();
                    result.add(KeyValue.pair(value.toUpperCase(), 1000));
                    result.add(KeyValue.pair(value.toLowerCase(), 9000));
                    return result;
                }
        );
    }

    // input record <K,V> can be transformed into output records <K:V'>, <K:V''>, ...
    //The example below splits input records <null:String> containing sentences as values into their words.
    public void flatMapValues() {
        KStream<byte[], String> inputStream = builder.stream("topic");
        KStream<byte[], String> outputStream = inputStream.flatMapValues(new ValueMapper<String, Iterable<String>>() {
            @Override
            public Iterable<String> apply(String value) {
                return Arrays.asList(value.split(" "));
            }
        });

        KStream<byte[], String> sentences = builder.stream(Serdes.ByteArray(), Serdes.String(), "topic");
        KStream<byte[], String> words = sentences.flatMapValues(value -> Arrays.asList(value.split("\\s+")));
    }

    public void groupByKey() {
        // Group by the existing key
        KGroupedStream<String, Long> groupedStream1 = wordCountStream.groupByKey(Serdes.String(), Serdes.Long());
        KGroupedStream<byte[], String> groupedStream2 = sentenceStream.groupByKey(Serdes.ByteArray(), Serdes.String());
    }

    public void groupByKStream() {
        //Groups the records by a new key, which may be of a different key type.
        // Group the stream by a new key
        KGroupedStream<String, String> groupedStream = sentenceStream.groupBy(
                // Input: byte[], String. Output: String, String
                (key, value) -> value, Serdes.String(), Serdes.String()
        );

        groupedStream = sentenceStream.groupBy(
                new KeyValueMapper<byte[], String, String>() {
                    @Override
                    public String apply(byte[] key, String value) {
                        return value;
                    }
                },Serdes.String(), Serdes.String());
    }

    //When grouping a table, you may also specify a new value and value type.
    //groupBy is a shorthand for selectKey(...).groupByKey()
    public void groupByKTable() {
        // Group the table by a new key and key type, and also modify the value and value type.
        KTable<byte[], String> sentenceTable = builder.table(Serdes.ByteArray(), Serdes.String(), "word-counts-input-topic", "word-counts-partitioned-store");

        KGroupedTable<String, Integer> groupedTable = sentenceTable.groupBy(
                // Input: byte[], String. Output: String, Integer
                (key, value) -> KeyValue.pair(value, value.length()), Serdes.String(), Serdes.Integer()
        );

        groupedTable = sentenceTable.groupBy(
                new KeyValueMapper<byte[], String, KeyValue<String, Integer>>() {
                    @Override
                    public KeyValue<String, Integer> apply(byte[] key, String value) {
                        return KeyValue.pair(value, value.length());
                    }
                },
                Serdes.String(), /* key (note: type was modified) */
                Serdes.Integer() /* value (note: type was modified) */
        );
    }

}
