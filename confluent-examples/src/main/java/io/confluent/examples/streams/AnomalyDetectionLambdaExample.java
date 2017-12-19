/*
 * Copyright Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.confluent.examples.streams;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Demonstrates how to count things over time, using time windows. In this specific example we
 * read from a user click stream and detect any such users as anomalous that have appeared more
 * than twice in the click stream during one minute.
 * <p>
 * Note: This example uses lambda expressions and thus works with Java 8+ only.
 * <p>
 * <br>
 * HOW TO RUN THIS EXAMPLE
 * <p>
 * 1) Start Zookeeper and Kafka. Please refer to <a href='http://docs.confluent.io/current/quickstart.html#quickstart'>QuickStart</a>.
 * <p>
 * 2) Create the input and output topics used by this example.
 * <pre>
 * {@code
 * $ bin/kafka-topics --create --topic UserClicks \
 *                    --zookeeper localhost:2181 --partitions 1 --replication-factor 1
 * $ bin/kafka-topics --create --topic AnomalousUsers \
 *                    --zookeeper localhost:2181 --partitions 1 --replication-factor 1
 * }</pre>
 * Note: The above commands are for the Confluent Platform. For Apache Kafka it should be {@code bin/kafka-topics.sh ...}.
 * <p>
 * 3) Start this example application either in your IDE or on the command line.
 * <p>
 * If via the command line please refer to <a href='https://github.com/confluentinc/kafka-streams-examples#packaging-and-running'>Packaging</a>.
 * Once packaged you can then run:
 * <pre>
 * {@code
 * $ java -cp target/kafka-streams-examples-4.0.0-SNAPSHOT-standalone.jar io.confluent.examples.streams.AnomalyDetectionLambdaExample
 * }</pre>
 * <p>
 * 4) Write some input data to the source topic (e.g. via {@code kafka-console-producer}. The already
 * running example application (step 3) will automatically process this input data and write the
 * results to the output topic.
 * <pre>
 * {@code
 * # Start the console producer. You can then enter input data by writing some line of text,
 * # followed by ENTER.  The input data you enter should be some example usernames; and because
 * # this example is set to detect only such users as "anomalous" that appear at least three times
 * # during a 1-minute time window, you should enter at least one username three times&mdash;otherwise
 * # this example won't produce any output data (cf. step 5).
 * #
 * #   alice<ENTER>
 * #   alice<ENTER>
 * #   bob<ENTER>
 * #   alice<ENTER>
 * #   alice<ENTER>
 * #   charlie<ENTER>
 * #
 * # Every line you enter will become the value of a single Kafka message.
 * $ bin/kafka-console-producer --broker-list localhost:9092 --topic UserClicks
 * }</pre>
 * 5) Inspect the resulting data in the output topic, e.g. via {@code kafka-console-consumer}.
 * <pre>
 * {@code
 * $ bin/kafka-console-consumer --topic AnomalousUsers --from-beginning \
 *                              --new-consumer --bootstrap-server localhost:9092 \
 *                              --property print.key=true \
 *                              --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
 * }</pre>
 * You should see output data similar to:
 * <pre>
 * {@code
 * [alice@1466521140000]	4
 * }</pre>
 * Here, the output format is "[USER@WINDOW_START_TIME] COUNT".
 * <p>
 * 6) Once you're done with your experiments, you can stop this example via {@code Ctrl-C}.  If needed,
 * also stop the Kafka broker ({@code Ctrl-C}), and only then stop the ZooKeeper instance ({@code Ctrl-C}).
 */
public class AnomalyDetectionLambdaExample {

  public static void main(final String[] args) throws Exception {
    final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
    final Properties streamsConfiguration = new Properties();
    // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
    // against which the application is run.
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "anomaly-detection-lambda-example");
    streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "anomaly-detection-lambda-example-client");
    // Where to find Kafka broker(s).
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    // Specify default (de)serializers for record keys and for record values.
    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    // Set the commit interval to 500ms so that any changes are flushed frequently. The low latency
    // would be important for anomaly detection.
    streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 500);

    final Serde<String> stringSerde = Serdes.String();
    final Serde<Long> longSerde = Serdes.Long();

    final StreamsBuilder builder = new StreamsBuilder();

    // Read the source stream.  In this example, we ignore whatever is stored in the record key and
    // assume the record value contains the username (and each record would represent a single
    // click by the corresponding user).
    final KStream<String, String> views = builder.stream("UserClicks");

    final KTable<Windowed<String>, Long> anomalousUsers = views
      // map the user name as key, because the subsequent counting is performed based on the key
      .map((ignoredKey, username) -> new KeyValue<>(username, username))
      // count users, using one-minute tumbling windows;
      // no need to specify explicit serdes because the resulting key and value types match our default serde settings
      .groupByKey()
      .windowedBy(TimeWindows.of(TimeUnit.MINUTES.toMillis(1)))
      .count()
      // get users whose one-minute count is >= 3
      .filter((windowedUserId, count) -> count >= 3);

    // Note: The following operations would NOT be needed for the actual anomaly detection,
    // which would normally stop at the filter() above.  We use the operations below only to
    // "massage" the output data so it is easier to inspect on the console via
    // kafka-console-consumer.
    final KStream<String, Long> anomalousUsersForConsole = anomalousUsers
      // get rid of windows (and the underlying KTable) by transforming the KTable to a KStream
      .toStream()
      // sanitize the output by removing null record values (again, we do this only so that the
      // output is easier to read via kafka-console-consumer combined with LongDeserializer
      // because LongDeserializer fails on null values, and even though we could configure
      // kafka-console-consumer to skip messages on error the output still wouldn't look pretty)
      .filter((windowedUserId, count) -> count != null)
      .map((windowedUserId, count) -> new KeyValue<>(windowedUserId.toString(), count));

    // write to the result topic
    anomalousUsersForConsole.to("AnomalousUsers", Produced.with(stringSerde, longSerde));

    final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
    // Always (and unconditionally) clean local state prior to starting the processing topology.
    // We opt for this unconditional call here because this will make it easier for you to play around with the example
    // when resetting the application for doing a re-run (via the Application Reset Tool,
    // http://docs.confluent.io/current/streams/developer-guide.html#application-reset-tool).
    //
    // The drawback of cleaning up local state prior is that your app must rebuilt its local state from scratch, which
    // will take time and will require reading all the state-relevant data from the Kafka cluster over the network.
    // Thus in a production scenario you typically do not want to clean up always as we do here but rather only when it
    // is truly needed, i.e., only under certain conditions (e.g., the presence of a command line flag for your app).
    // See `ApplicationResetExample.java` for a production-like example.
    streams.cleanUp();
    streams.start();

    // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }

}
