package kafka.streams;

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
public class StreamStatefulDemo {
    static KStreamBuilder builder = new KStreamBuilder();
    KStream<String, String> stream = builder.stream(Serdes.String(), Serdes.String(), "input-topic");

    public void testWordcount() {
        KTable<String, Long> wordCounts = stream
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
                .count("Counts");

        wordCounts = stream
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
                }).count("Counts");

        // 实现wordcount的其他几种方式
        // groupBy((key, word) -> word) 等价于
        // 1. map((key,word) -> KeyValue.pair(word,word)).groupByKey()
        // 2. selectKey((key,word) -> word).groupByKey()
        wordCounts = stream.flatMapValues(value -> Arrays.asList(value.split("")))
                .map((key,word) -> KeyValue.pair(word,word))
                .groupByKey().count("Counts");

        wordCounts = stream.flatMapValues(value -> Arrays.asList(value.split("")))
                .selectKey((key,word) -> word)
                .groupByKey().count("Counts");

        // Convert the `KTable<String, Long>` into a `KStream<String, Long>`.
        KStream<String, Long> wordCountStream = wordCounts.toStream();
    }

    public void testWCStepByStep() {
        // 句子流
        KStream<byte[], String> sentences = builder.stream(
                Serdes.ByteArray(), Serdes.String(), "sentence-topic");

        // 将句子转成一个个单词
        KStream<byte[], String> words = sentences.flatMapValues(
                value -> Arrays.asList(value.split("\\s+")));

        // 输出到新的主题, 单词流
        words.to("word-input");

        // 转换成小写
        KStream<byte[], String> lowerWords = words.mapValues(
                value -> value.toLowerCase());

        // 对键值进行转换, 更改值的类型
        KStream<String, Integer> _words2 = lowerWords.map(
                (key, value) -> KeyValue.pair(value, value.length()));

        // 根据键分组
        KGroupedStream<String, String> groups = lowerWords.groupBy((key,word) -> word);
        groups = lowerWords.groupBy((key,word) -> word);
        groups = lowerWords.map((key,word) -> KeyValue.pair(word, word)).groupByKey();
        groups = lowerWords.selectKey((key,word) -> word).groupByKey();

        // 对分组流(KGroupedStream)的每个单词进行计数
        KTable<String, Long> counts = groups.count("Counts1");

        // 对表进行过滤
        KTable<String, Long> filter = counts.filter((key,value) -> value > 2);

        // 构造表
        KTable<byte[], String> table = builder.table(
                Serdes.ByteArray(), Serdes.String(), "word-input", "wc-store");

        // 对分组表(KGroupedTable)进行单词计数
        counts = table.mapValues(value -> value.toLowerCase())
                .groupBy((key,value) -> KeyValue.pair(value, value))
                .count("Counts2");
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

    /**
     *
     * For stream-stream joins it’s important to highlight that
     * a new input record on one side will produce a join output for each matching record on the other side,
     * and there can be multiple such matching records in a given join window
     *
     * INNER JOIN(join), LEFT JOIN(leftJoin), RIGHT JOIN(outerJoin) both has this feature:
     *
     * The join is key-based, i.e. with the join predicate leftRecord.key == rightRecord.key,
     * and window-based, i.e. two input records are joined if and only if their timestamps
     * are “close” to each other as defined by the user-supplied JoinWindows,
     * i.e. the window defines an additional join predicate over the record timestamps.
     *
     * The join will be triggered under the conditions listed below whenever new input is received.
     * When it is triggered, the user-supplied ValueJoiner will be called to produce join output records.
     *
     * Input records with a null key or a null value are ignored and do not trigger the join.
     *
     * -- FOR LEFT JOIN
     *
     * For each input record on the left side that does not have any match on the right side,
     * the ValueJoiner will be called with ValueJoiner#apply(leftRecord.value, null)
     *
     * -- FOR RIGHT JOIN
     *
     * For each input record on one side that does not have any match on the other side,
     * the ValueJoiner will be called with ValueJoiner#apply(leftRecord.value, null) or ValueJoiner#apply(null, rightRecord.value)
     */
    public void testStreamJoin() {
        KStream<String, Long> left = builder.stream(Serdes.String(), Serdes.Long(), "input-join-topic1");;
        KStream<String, Double> right = builder.stream(Serdes.String(), Serdes.Double(), "input-join-topic2");;

        KStream<String, String> joined = left.join(right,
                (leftValue, rightValue) -> "left=" + leftValue + ", right=" + rightValue, /* ValueJoiner */
                //JoinWindows.of(TimeUnit.MINUTES.toMillis(5)),
                JoinWindows.of(TimeUnit.MINUTES.toMillis(5)).before(10*1000).after(5*1000),
                Serdes.String(), /* key */
                Serdes.Long(),   /* left value */
                Serdes.Double()  /* right value */
        );

        joined = left.join(right,
                new ValueJoiner<Long, Double, String>() {
                    @Override
                    public String apply(Long leftValue, Double rightValue) {
                        return "left=" + leftValue + ", right=" + rightValue;
                    }
                },
                JoinWindows.of(TimeUnit.MINUTES.toMillis(5)),
                Serdes.String(), /* key */
                Serdes.Long(),   /* left value */
                Serdes.Double()  /* right value */
        );
    }

    /**
     * INNER JOIN
     *
     * Input records with a null key are ignored and do not trigger the join.
     * Input records with a null value are interpreted as tombstones for the corresponding key,
     * which indicate the deletion of the key from the table.
     * Tombstones do not trigger the join.
     * When an input tombstone is received, then an output tombstone is forwarded directly to the join result KTable if required
     * (i.e. only if the corresponding key actually exists already in the join result KTable).
     */
    public void testTableJoin() {
        KTable<String, Long> left = builder.table(Serdes.String(), Serdes.Long(), "input-join-topic1", "store-1");
        KTable<String, Double> right = builder.table(Serdes.String(), Serdes.Double(), "input-join-topic2", "store-2");

        KTable<String, String> joined = left.join(right,
                (leftValue, rightValue) -> "left=" + leftValue + ", right=" + rightValue /* ValueJoiner */
        );

        joined = left.join(right,
                new ValueJoiner<Long, Double, String>() {
                    @Override
                    public String apply(Long leftValue, Double rightValue) {
                        return "left=" + leftValue + ", right=" + rightValue;
                    }
                });
    }

    /**
     * perform table lookups against a KTable (changelog stream) upon receiving a new record from the KStream (record stream)
     *
     * Only input records for the left side (stream) trigger the join.
     * Input records for the right side (table) update only the internal right-side join state.
     *
     * Input records for the stream with a null key or a null value are ignored and do not trigger the join.
     *
     * Input records for the table with a null value are interpreted as tombstones for the corresponding key,
     * which indicate the deletion of the key from the table. Tombstones do not trigger the join.
     */
    public void testStreamTableJoin() {
        KStream<String, Long> left = builder.stream(Serdes.String(), Serdes.Long(), "input-join-topic1");;
        KTable<String, Double> right = builder.table(Serdes.String(), Serdes.Double(), "input-join-topic2", "store-2");

        KStream<String, String> joined = left.join(right,
                (leftValue, rightValue) -> "left=" + leftValue + ", right=" + rightValue, /* ValueJoiner */
                Serdes.String(), /* key */
                Serdes.Long()    /* left value */
        );

        joined = left.join(right,
                new ValueJoiner<Long, Double, String>() {
                    @Override
                    public String apply(Long leftValue, Double rightValue) {
                        return "left=" + leftValue + ", right=" + rightValue;
                    }
                },
                Serdes.String(), /* key */
                Serdes.Long()    /* left value */
        );
    }

    public void testStreamGlobalTableJoin() {
        KStream<String, Long> left = builder.stream(Serdes.String(), Serdes.Long(), "input-join-topic1");;
        GlobalKTable<Integer, Double> right = builder.globalTable(Serdes.Integer(), Serdes.Double(), "input-join-topic2", "store-2");;

        KStream<String, String> joined = left.join(right,
                (leftKey, leftValue) -> leftKey.length(), /* derive a (potentially) new key by which to lookup against the table */
                (leftValue, rightValue) -> "left=" + leftValue + ", right=" + rightValue /* ValueJoiner */
        );

        joined = left.join(right,
                new KeyValueMapper<String, Long, Integer>() { /* derive a (potentially) new key by which to lookup against the table */
                    @Override
                    public Integer apply(String key, Long value) {
                        return key.length();
                    }
                },
                new ValueJoiner<Long, Double, String>() {
                    @Override
                    public String apply(Long leftValue, Double rightValue) {
                        return "left=" + leftValue + ", right=" + rightValue;
                    }
                });
    }
}
