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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

/**
 * Demonstrates how to reset a Kafka Streams application to re-process its input data from scratch.
 * See also <a href='http://docs.confluent.io/current/streams/developer-guide.html#application-reset-tool'>http://docs.confluent.io/current/streams/developer-guide.html#application-reset-tool</a>
 * <p>
 * The main purpose of the example is to explain the usage of the "Application Reset Tool".
 * Thus, we don’t put the focus on what this topology is actually doing&mdash;the point is to have an example of a
 * typical topology that has input topics, intermediate topics, and output topics.
 * One important part in the code is the call to {@link KafkaStreams#cleanUp()}.
 * This call performs a local application (instance) reset and must be part in the code to make the application "reset ready".
 * <p>
 * <br>
 * HOW TO RUN THIS EXAMPLE
 * <p>
 * 1) Start Zookeeper and Kafka. Please refer to <a href='http://docs.confluent.io/current/quickstart.html#quickstart'>QuickStart</a>.
 * <p>
 * 2) Create the input, intermediate, and output topics used by this example.
 * <pre>
 * {@code
 * $ bin/kafka-topics --create --topic my-input-topic \
 *                    --zookeeper localhost:2181 --partitions 1 --replication-factor 1
 * $ bin/kafka-topics --create --topic rekeyed-topic \
 *                    --zookeeper localhost:2181 --partitions 1 --replication-factor 1
 * $ bin/kafka-topics --create --topic my-output-topic \
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
 * $ java -cp target/kafka-streams-examples-4.0.0-SNAPSHOT-standalone.jar io.confluent.examples.streams.ApplicationResetExample
 * }
 * </pre>
 * 4) Write some input data to the source topic (e.g. via {@code kafka-console-producer}).
 * The already running example application (step 3) will automatically process this input data and write the results to the output topics.
 * <pre>
 * {@code
 * # Start the console producer. You can then enter input data by writing some line of text, followed by ENTER:
 * #
 * #    hello world<ENTER>
 * #    hello kafka streams<ENTER>
 * #    all streams lead to kafka<ENTER>
 * #
 * # Every line you enter will become the value of a single Kafka message.
 * $ bin/kafka-console-producer --broker-list localhost:9092 --topic my-input-topic
 * }</pre>
 * 5) Inspect the resulting data in the output topic, e.g. via {@code kafka-console-consumer}.
 * <pre>
 * {@code
 * $ bin/kafka-console-consumer --topic my-output-topic --from-beginning \
 *                              --new-consumer --bootstrap-server localhost:9092 \
 *                              --property print.key=true \
 *                              --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
 * }</pre>
 * You should see output data similar to:
 * <pre>
 * {@code
 *     hello 1
 *     hello 2
 *     all 1
 * }</pre>
 * 6) Now you can stop the Streams application via {@code Ctrl-C}.
 * <p>
 * 7) In this step we will <strong>reset</strong> the application.
 * The effect is that, once the application has been restarted (which we are going to do in step 8),
 * it will reprocess its input data by re-reading the input topic.
 * To reset your application you must run:
 * <pre>
 * {@code
 * $ bin/kafka-streams-application-reset --application-id application-reset-demo \
 *                                       --input-topics my-input-topic \
 *                                       --intermediate-topics rekeyed-topic
 * }</pre>
 * If you were to restart your application without resetting it,
 * then the application would resume reading from the point where it was stopped in step 6
 * (rather than re-reading the input topic).
 * In this case, the restarted application would idle and wait for you to write new input data to its input topic.
 * <p>
 * 8) A full application reset also requires a "local cleanup".
 * In this example application, you need to specify the command line argument {@code --reset}
 * to tell the application to perform such a local cleanup by calling {@link KafkaStreams#cleanUp()}
 * before it begins processing.
 * Thus, restart the application via:
 * <pre>
 * {@code
 * $ java -cp target/kafka-streams-examples-4.0.0-SNAPSHOT-standalone.jar io.confluent.examples.streams.ApplicationResetExample --reset
 * }</pre>
 * 9) If your console consumer (from step 5) is still running, you should see the same output data again.
 * If it was stopped and you restart it, if will print the result "twice".
 * Resetting an application does not modify output topics, thus, for this example, the output topic will contain the result twice.
 * <p>
 * 10) Once you're done with your experiments, you can stop this example via {@code Ctrl-C}.
 * If needed, also stop the Kafka broker ({@code Ctrl-C}), and only then stop the ZooKeeper instance ({@code Ctrl-C}).
 */
public class ApplicationResetExample {

  public static void main(final String[] args) throws Exception {
    final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
    // Kafka Streams configuration
    final Properties streamsConfiguration = new Properties();
    // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
    // against which the application is run.
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "application-reset-demo");
    streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "application-reset-demo-client");
    // Where to find Kafka broker(s).
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    // Specify default (de)serializers for record keys and for record values.
    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    // Read the topic from the very beginning if no previous consumer offsets are found for this app.
    // Resetting an app will set any existing consumer offsets to zero,
    // so setting this config combined with resetting will cause the application to re-process all the input data in the topic.
    streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    final KafkaStreams streams = run(args, streamsConfiguration);

    // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }

  public static KafkaStreams run(final String[] args, final Properties streamsConfiguration) {
    // Define the processing topology
    final StreamsBuilder builder = new StreamsBuilder();
    final KStream<String, String> input = builder.stream("my-input-topic");
    input.selectKey((key, value) -> value.split(" ")[0])
      .groupByKey()
      .count()
      .toStream()
      .to("my-output-topic", Produced.with(Serdes.String(), Serdes.Long()));

    final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);

    // Delete the application's local state on reset
    if (args.length > 0 && args[0].equals("--reset")) {
      streams.cleanUp();
    }

    streams.start();

    return streams;
  }

}
