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

import java.util.Properties;

/**
 * Demonstrates group-by operations and aggregations on KTable. In this specific example we
 * compute the user count per geo-region from a KTable that contains {@code <user, region>} information.
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
 * $ bin/kafka-topics --create --topic UserRegions \
 *                    --zookeeper localhost:2181 --partitions 1 --replication-factor 1
 * $ bin/kafka-topics --create --topic LargeRegions \
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
 * $ java -cp target/kafka-streams-examples-4.0.0-SNAPSHOT-standalone.jar io.confluent.examples.streams.UserRegionLambdaExample
 * }
 * </pre>
 * 4) Write some input data to the source topics (e.g. via {@code kafka-console-producer}). The already
 * running example application (step 3) will automatically process this input data and write the
 * results to the output topic.
 * <pre>
 * {@code
 * # Start the console producer, then input some example data records. The input data you enter
 * # should be in the form of USER,REGION<ENTER> and, because this example is set to discard any
 * # regions that have a user count of only 1, at least one region should have two users or more --
 * # otherwise this example won't produce any output data (cf. step 5).
 * #
 * # alice,asia<ENTER>
 * # bob,americas<ENTER>
 * # chao,asia<ENTER>
 * # dave,europe<ENTER>
 * # alice,europe<ENTER>        <<< Note: Alice moved from Asia to Europe
 * # eve,americas<ENTER>
 * # fang,asia<ENTER>
 * # gandalf,europe<ENTER>
 * #
 * # Here, the part before the comma will become the message key, and the part after the comma will
 * # become the message value.
 * $ bin/kafka-console-producer --broker-list localhost:9092 --topic UserRegions \
 *                              --property parse.key=true --property key.separator=,
 * }</pre>
 * 5) Inspect the resulting data in the output topics, e.g. via {@code kafka-console-consumer}.
 * <pre>
 * {@code
 * $ bin/kafka-console-consumer --topic LargeRegions --from-beginning \
 *                              --new-consumer --bootstrap-server localhost:9092 \
 *                              --property print.key=true \
 *                              --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
 * }</pre>
 * You should see output data similar to:
 * <pre>
 * {@code
 * americas 2     # because Bob and Eve are currently in Americas
 * asia     2     # because Chao and Fang are currently in Asia
 * europe   3     # because Dave, Alice, and Gandalf are currently in Europe
 * }</pre>
 * 6) Once you're done with your experiments, you can stop this example via {@code Ctrl-C}. If needed,
 * also stop the Kafka broker ({@code Ctrl-C}), and only then stop the ZooKeeper instance ({@code Ctrl-C}).
 */
public class UserRegionLambdaExample {

  public static void main(final String[] args) throws Exception {
    final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
    final Properties streamsConfiguration = new Properties();
    // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
    // against which the application is run.
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "user-region-lambda-example");
    streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "user-region-lambda-example-client");
    // Where to find Kafka broker(s).
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    // Specify default (de)serializers for record keys and for record values.
    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    // Records should be flushed every 10 seconds. This is less than the default
    // in order to keep this example interactive.
    streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);

    final Serde<String> stringSerde = Serdes.String();
    final Serde<Long> longSerde = Serdes.Long();

    final StreamsBuilder builder = new StreamsBuilder();

    final KTable<String, String> userRegions = builder.table("UserRegions");

    // Aggregate the user counts of by region
    final KTable<String, Long> regionCounts = userRegions
      // Count by region;
      // no need to specify explicit serdes because the resulting key and value types match our default serde settings
      .groupBy((userId, region) -> KeyValue.pair(region, region))
      .count()
      // discard any regions with only 1 user
      .filter((regionName, count) -> count >= 2);

    // Note: The following operations would NOT be needed for the actual users-per-region
    // computation, which would normally stop at the filter() above.  We use the operations
    // below only to "massage" the output data so it is easier to inspect on the console via
    // kafka-console-consumer.
    //
    final KStream<String, Long> regionCountsForConsole = regionCounts
      // get rid of windows (and the underlying KTable) by transforming the KTable to a KStream
      .toStream()
      // sanitize the output by removing null record values (again, we do this only so that the
      // output is easier to read via kafka-console-consumer combined with LongDeserializer
      // because LongDeserializer fails on null values, and even though we could configure
      // kafka-console-consumer to skip messages on error the output still wouldn't look pretty)
      .filter((regionName, count) -> count != null);

    // write to the result topic, we need to override the value serializer to for type long
    regionCountsForConsole.to("LargeRegions", Produced.with(stringSerde, longSerde));

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
