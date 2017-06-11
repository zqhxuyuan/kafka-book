package kafka.streams;

import junit.framework.TestCase;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * Created by zhengqh on 17/6/10.
 *
 * Stateful transformations, by definition, depend on state for processing inputs and producing outputs,
 * and hence implementation-wise they require a state store associated with the stream processor.
 * For example, in aggregating operations, a windowing state store is used to store the latest aggregation results per window;
 * in join operations, a windowing state store is used to store all the records received so far within the defined window boundary.
 */
public class StreamStatefulDemo extends TestCase{
    static KStreamBuilder builder = new KStreamBuilder();
    KStream<String, String> sentenceStream = builder.stream(Serdes.String(), Serdes.String(), "input-topic");

    @Override
    protected void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public void testWordcount() {
        KStream<String, Long> wordCounts = sentenceStream
                // Split each text line, by whitespace, into words.  The text lines are the record
                // values, i.e. we can ignore whatever data is in the record keys and thus invoke
                // `flatMapValues` instead of the more generic `flatMap`.
                .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
                // Group the stream by word to ensure the key of the record is the word.
                .groupBy((key, word) -> word)
                // Count the occurrences of each word (record key).
                //
                // This will change the stream type from `KGroupedStream<String, String>` to
                // `KTable<String, Long>` (word -> count).  We must provide a name for
                // the resulting KTable, which will be used to name e.g. its associated
                // state store and changelog topic.
                .count("Counts")
                // Convert the `KTable<String, Long>` into a `KStream<String, Long>`.
                .toStream();

        wordCounts = sentenceStream
                .flatMapValues(new ValueMapper<String, Iterable<String>>() {
                    @Override
                    public Iterable<String> apply(String value) {
                        return Arrays.asList(value.toLowerCase().split("\\W+"));
                    }
                })
                .groupBy(new KeyValueMapper<String, String, String>() {
                    @Override
                    public String apply(String key, String word) {
                        return word;
                    }
                }).count("Counts")
                .toStream();


        // 实现wordcount的其他几种方式
        // groupBy((key, word) -> word) 等价于
        // 1. map((key,word) -> KeyValue.pair(word,word)).groupByKey()
        // 2. selectKey((key,word) -> word).groupByKey()
        wordCounts = sentenceStream.flatMapValues(value -> Arrays.asList(value.split("")))
                .map((key,word) -> KeyValue.pair(word,word))
                .groupByKey().count("Counts").toStream();

        wordCounts = sentenceStream.flatMapValues(value -> Arrays.asList(value.split("")))
                .selectKey((key,word) -> word)
                .groupByKey().count("Counts").toStream();
    }

    public void testAggregate() {
        KStream<byte[], String> stream = builder.stream(Serdes.ByteArray(), Serdes.String(), "input-topic");

        KGroupedStream<byte[], String> groupedStream =
                stream.flatMapValues(value -> Arrays.asList(value.split("")))
                        .map((key,word) -> KeyValue.pair(word.getBytes(),word))
                        .groupByKey();

        // Aggregating a KGroupedStream (note how the value type changes from String to Long)
        KTable<byte[], Long> aggregatedStream = groupedStream.aggregate(
                () -> 0L, /* initializer */
                (aggKey, newValue, aggValue) -> aggValue + newValue.length(), /* adder */
                Serdes.Long(), /* serde for aggregate value */
                "aggregated-stream-store" /* state store name */);

        aggregatedStream = groupedStream.aggregate(
                new Initializer<Long>() { /* initializer */
                    @Override
                    public Long apply() {
                        return 0L;
                    }
                },
                new Aggregator<byte[], String, Long>() { /* adder */
                    @Override
                    public Long apply(byte[] aggKey, String newValue, Long aggValue) {
                        return aggValue + newValue.length();
                    }
                },
                Serdes.Long(),
                "aggregated-stream-store");

        // Aggregating a KGroupedTable (note how the value type changes from String to Long)
        stream.flatMapValues(value -> Arrays.asList(value.split(""))).map((key,word) -> KeyValue.pair(word.getBytes(),word)).to("input-topic2");
        KTable<byte[], String> table2 = builder.table(Serdes.ByteArray(), Serdes.String(), "input-topic2", "store");
        KGroupedTable<byte[], String> groupedTable2 = table2.groupBy((key, value) -> KeyValue.pair(key, value));

        KTable<byte[], Long> aggregatedTable2 = groupedTable2.aggregate(
                () -> 0L, /* initializer */
                (aggKey, newValue, aggValue) -> aggValue + newValue.length(), /* adder */
                (aggKey, oldValue, aggValue) -> aggValue - oldValue.length(), /* subtractor */
                Serdes.Long(), /* serde for aggregate value */
                "aggregated-table-store" /* state store name */);

        aggregatedTable2 = groupedTable2.aggregate(
                new Initializer<Long>() { /* initializer */
                    @Override
                    public Long apply() {
                        return 0L;
                    }
                },
                new Aggregator<byte[], String, Long>() { /* adder */
                    @Override
                    public Long apply(byte[] aggKey, String newValue, Long aggValue) {
                        return aggValue + newValue.length();
                    }
                },
                new Aggregator<byte[], String, Long>() { /* subtractor */
                    @Override
                    public Long apply(byte[] aggKey, String oldValue, Long aggValue) {
                        return aggValue - oldValue.length();
                    }
                },
                Serdes.Long(),
                "aggregated-table-store");

        // input-topic的每个值都只有一个word, 而不是sentence
        KTable<String, String> table = builder.table(Serdes.String(), Serdes.String(), "input-topic", "store");
        KGroupedTable<String, String> groupedTable = table.groupBy(
                (key, value) -> KeyValue.pair(value, value)
        );
        KTable<String, Long> aggregatedTable = groupedTable.aggregate(
                () -> 0L,
                (aggKey, newValue, aggValue) -> aggValue + newValue.length(),
                (aggKey, oldValue, aggValue) -> aggValue - oldValue.length(),
                Serdes.Long(), "aggregated-table-store");

    }

    public void testAggregateWindow() {
        KStream<String, Long> stream = builder.stream(Serdes.String(), Serdes.Long(), "input-topic");
        KGroupedStream<String, Long> groupedStream = stream.groupByKey();

        // Java 8+ examples, using lambda expressions

        // Aggregating with time-based windowing (here: with 5-minute tumbling windows)
        KTable<Windowed<String>, Long> timeWindowedAggregatedStream = groupedStream.aggregate(
                () -> 0L, /* initializer */
                (aggKey, newValue, aggValue) -> aggValue + newValue, /* adder */
                TimeWindows.of(TimeUnit.MINUTES.toMillis(5)), /* time-based window */
                Serdes.Long(), /* serde for aggregate value */
                "time-windowed-aggregated-stream-store" /* state store name */);

        // Aggregating with session-based windowing (here: with an inactivity gap of 5 minutes)
        KTable<Windowed<String>, Long> sessionizedAggregatedStream = groupedStream.aggregate(
                () -> 0L, /* initializer */
                (aggKey, newValue, aggValue) -> aggValue + newValue, /* adder */
                (aggKey, leftAggValue, rightAggValue) -> leftAggValue + rightAggValue, /* session merger */
                SessionWindows.with(TimeUnit.MINUTES.toMillis(5)), /* session window */
                Serdes.Long(), /* serde for aggregate value */
                "sessionized-aggregated-stream-store" /* state store name */);

        // Java 7 examples

        // Aggregating with time-based windowing (here: with 5-minute tumbling windows)
        timeWindowedAggregatedStream = groupedStream.aggregate(
                new Initializer<Long>() { /* initializer */
                    @Override
                    public Long apply() {
                        return 0L;
                    }
                },
                new Aggregator<String, Long, Long>() { /* adder */
                    @Override
                    public Long apply(String aggKey, Long newValue, Long aggValue) {
                        return aggValue + newValue;
                    }
                },
                TimeWindows.of(TimeUnit.MINUTES.toMillis(5)), /* time-based window */
                Serdes.Long(), /* serde for aggregate value */
                "time-windowed-aggregated-stream-store" /* state store name */);

        // Aggregating with session-based windowing (here: with an inactivity gap of 5 minutes)
        sessionizedAggregatedStream = groupedStream.aggregate(
                new Initializer<Long>() { /* initializer */
                    @Override
                    public Long apply() {
                        return 0L;
                    }
                },
                new Aggregator<String, Long, Long>() { /* adder */
                    @Override
                    public Long apply(String aggKey, Long newValue, Long aggValue) {
                        return aggValue + newValue;
                    }
                },
                new Merger<String, Long>() { /* session merger */
                    @Override
                    public Long apply(String aggKey, Long leftAggValue, Long rightAggValue) {
                        return rightAggValue + leftAggValue;
                    }
                },
                SessionWindows.with(TimeUnit.MINUTES.toMillis(5)), /* session window */
                Serdes.Long(), /* serde for aggregate value */
                "sessionized-aggregated-stream-store" /* state store name */);

    }

    public void testCountAndWindow() {
        KStream<String, Long> stream = builder.stream(Serdes.String(), Serdes.Long(), "input-topic");
        KTable<String, Long> table = builder.table(Serdes.String(), Serdes.Long(), "input-topic", "store");

        KGroupedStream<String, Long> groupedStream = stream.groupByKey();
        KGroupedTable<String, Long> groupedTable = table.groupBy(
                (key, value) -> KeyValue.pair(key, value)
        );

        // Counting a KGroupedStream
        KTable<String, Long> aggregatedStream = groupedStream.count("counted-stream-store");

        // Counting a KGroupedTable
        KTable<String, Long> aggregatedTable = groupedTable.count("counted-table-store");

        // Counting a KGroupedStream with time-based windowing (here: with 5-minute tumbling windows)
        KTable<Windowed<String>, Long> table2 = groupedStream.count(
                TimeWindows.of(TimeUnit.MINUTES.toMillis(5)), /* time-based window */
                "time-windowed-counted-stream-store");

        // Counting a KGroupedStream with session-based windowing (here: with 5-minute inactivity gaps)
        KTable<Windowed<String>, Long> table3 = groupedStream.count(
                SessionWindows.with(TimeUnit.MINUTES.toMillis(5)), /* session window */
                "sessionized-counted-stream-store");
    }

    public void testReduce() {
        KStream<String, Long> stream = builder.stream(Serdes.String(), Serdes.Long(), "input-topic");
        KTable<String, Long> table = builder.table(Serdes.String(), Serdes.Long(), "input-topic", "store");

        KGroupedStream<String, Long> groupedStream = stream.groupByKey();
        KGroupedTable<String, Long> groupedTable = table.groupBy(
                (key, value) -> KeyValue.pair(key, value)
        );

        // Reducing a KGroupedStream
        KTable<String, Long> aggregatedStream = groupedStream.reduce(
                (aggValue, newValue) -> aggValue + newValue, /* adder */
                "reduced-stream-store" /* state store name */);

        // Reducing a KGroupedTable
        KTable<String, Long> aggregatedTable = groupedTable.reduce(
                (aggValue, newValue) -> aggValue + newValue, /* adder */
                (aggValue, oldValue) -> aggValue - oldValue, /* subtractor */
                "reduced-table-store" /* state store name */);


        // Java 7 examples

        // Reducing a KGroupedStream
        aggregatedStream = groupedStream.reduce(
                new Reducer<Long>() { /* adder */
                    @Override
                    public Long apply(Long aggValue, Long newValue) {
                        return aggValue + newValue;
                    }
                },
                "reduced-stream-store" /* state store name */);

        // Reducing a KGroupedTable
        aggregatedTable = groupedTable.reduce(
                new Reducer<Long>() { /* adder */
                    @Override
                    public Long apply(Long aggValue, Long newValue) {
                        return aggValue + newValue;
                    }
                },
                new Reducer<Long>() { /* subtractor */
                    @Override
                    public Long apply(Long aggValue, Long oldValue) {
                        return aggValue - oldValue;
                    }
                },
                "reduced-table-store" /* state store name */);
    }

    public void testReduceWindow() {
        KStream<String, Long> stream = builder.stream(Serdes.String(), Serdes.Long(), "input-topic");
        KTable<String, Long> table = builder.table(Serdes.String(), Serdes.Long(), "input-topic", "store");

        KGroupedStream<String, Long> groupedStream = stream.groupByKey();
        KGroupedTable<String, Long> groupedTable = table.groupBy(
                (key, value) -> KeyValue.pair(key, value)
        );


        // Java 8+ examples, using lambda expressions

        // Aggregating with time-based windowing (here: with 5-minute tumbling windows)
        KTable<Windowed<String>, Long> timeWindowedAggregatedStream = groupedStream.reduce(
                (aggValue, newValue) -> aggValue + newValue, /* adder */
                TimeWindows.of(TimeUnit.MINUTES.toMillis(5)), /* time-based window */
                "time-windowed-reduced-stream-store" /* state store name */);

        // Aggregating with session-based windowing (here: with an inactivity gap of 5 minutes)
        KTable<Windowed<String>, Long> sessionzedAggregatedStream = groupedStream.reduce(
                (aggValue, newValue) -> aggValue + newValue, /* adder */
                SessionWindows.with(TimeUnit.MINUTES.toMillis(5)), /* session window */
                "sessionized-reduced-stream-store" /* state store name */);


        // Java 7 examples

        // Aggregating with time-based windowing (here: with 5-minute tumbling windows)
        timeWindowedAggregatedStream = groupedStream.reduce(
                new Reducer<Long>() { /* adder */
                    @Override
                    public Long apply(Long aggValue, Long newValue) {
                        return aggValue + newValue;
                    }
                },
                TimeWindows.of(TimeUnit.MINUTES.toMillis(5)), /* time-based window */
                "time-windowed-reduced-stream-store" /* state store name */);

        // Aggregating with session-based windowing (here: with an inactivity gap of 5 minutes)
        timeWindowedAggregatedStream = groupedStream.reduce(
                new Reducer<Long>() { /* adder */
                    @Override
                    public Long apply(Long aggValue, Long newValue) {
                        return aggValue + newValue;
                    }
                },
                SessionWindows.with(TimeUnit.MINUTES.toMillis(5)), /* session window */
                "sessionized-reduced-stream-store" /* state store name */);
    }
}
