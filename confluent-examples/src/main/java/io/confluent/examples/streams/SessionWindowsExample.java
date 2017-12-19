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

import io.confluent.examples.streams.avro.PlayEvent;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.state.SessionStore;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Demonstrates counting user activity (play-events) into Session Windows
 * <p>
 * In this example we count play-events by session. We define a session as events
 * received by a user that all fall within a specified gap of inactivity. In this case,
 * 30 minutes. The sessions are constantly aggregated into the StateStore "play-events-per-session",
 * they are also output to a topic with the same name.
 * <p>
 * <br>
 * HOW TO RUN THIS EXAMPLE
 * <p>
 * 1) Start Zookeeper, Kafka, and Confluent Schema Registry. Please refer to <a href='http://docs.confluent.io/current/quickstart.html#quickstart'>QuickStart</a>.
 * <p>
 * 2) Create the input/intermediate/output topics used by this example.
 * <pre>
 * {@code
 * $ bin/kafka-topics --create --topic play-events \
 *                    --zookeeper localhost:2181 --partitions 1 --replication-factor 1
 * $ bin/kafka-topics --create --topic play-events-per-session \
 *                    --zookeeper localhost:2181 --partitions 1 --replication-factor 1
 * }
 * </pre>
 * Note: The above commands are for the Confluent Platform. For Apache Kafka it should be
 * `bin/kafka-topics.sh ...`.
 * <p>
 * 3) Start this example application either in your IDE or on the command line.
 * <p>
 * If via the command line please refer to <a href='https://github.com/confluentinc/kafka-streams-examples#packaging-and-running'>Packaging</a>.
 * Once packaged you can then run:
 * <pre>
 * {@code
 * $ java -cp target/kafka-streams-examples-4.0.0-SNAPSHOT-standalone.jar io.confluent.examples.streams.SessionWindowsExample
 * }
 * </pre>
 * 4) Write some input data to the source topics (e.g. via {@link SessionWindowsExampleDriver}). The
 * already running example application (step 3) will automatically process this input data and write
 * the results to the output topic.
 * <pre>
 * {@code
 * # Here: Write input data using the example driver. The driver will also consume, and print, the data from the output
 * topic. The driver will stop when it has received all output records
 * $ java -cp target/kafka-streams-examples-4.0.0-SNAPSHOT-standalone.jar io.confluent.examples.streams.SessionWindowsExampleDriver
 * }
 * </pre>
 * You should see output data similar to:
 * <pre>
 * {@code
 * jo@1484823406597->1484823406597 = 1          # new session for jo created
 * bill@1484823466597->1484823466597 = 1        # new session for bill created
 * sarah@1484823526597->1484823526597 = 1       # new session for sarah created
 * jo@1484825207597->1484825207597 = 1          # new session for jo created as event time is after inactivity gap
 * bill@1484823466597->1484825206597 = 2        # extend previous session for bill as event time is within inactivity gap
 * sarah@1484827006597->1484827006597 = 1       # new session for sarah created as event time is after inactivity gap
 * jo@1484823406597->1484825207597 = 3          # new event merges 2 previous sessions for jo
 * bill@1484828806597->1484828806597 = 1        # new session for bill created
 * sarah@1484827006597->1484827186597 = 2       # extend session for sarah as event time is within inactivity gap
 * }
 * </pre>
 * <p>
 * 5) Once you're done with your experiments, you can stop this example via {@code Ctrl-C}. If needed,
 * also stop the Confluent Schema Registry ({@code Ctrl-C}), then stop the Kafka broker ({@code Ctrl-C}), and
 * only then stop the ZooKeeper instance ({@code Ctrl-C}).
 * <p>
 * You can also take a look at io.confluent.examples.streams.SessionWindowsExampleTest for an example
 * of the expected outputs.
 */
public class SessionWindowsExample {

  static final String PLAY_EVENTS = "play-events";
  static final Long INACTIVITY_GAP = TimeUnit.MINUTES.toMillis(30);
  static final String PLAY_EVENTS_PER_SESSION = "play-events-per-session";

  public static void main(String[] args) {
    final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
    final String schemaRegistryUrl = args.length > 1 ? args[1] : "http://localhost:8081";
    final KafkaStreams streams = createStreams(bootstrapServers,
                                               schemaRegistryUrl,
                                               "/tmp/kafka-streams");

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

  static KafkaStreams createStreams(final String bootstrapServers,
                                    final String schemaRegistryUrl,
                                    final String stateDir) {
    final Properties config = new Properties();
    // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
    // against which the application is run.
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "session-windows-example");
    config.put(StreamsConfig.CLIENT_ID_CONFIG, "session-windows-example-client");
    // Where to find Kafka broker(s).
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    config.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
    // Set to earliest so we don't miss any data that arrived in the topics before the process
    // started
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    // disable caching to see session merging
    config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

    // create and configure the SpecificAvroSerdes required in this example
    final SpecificAvroSerde<PlayEvent> playEventSerde = new SpecificAvroSerde<>();
    final Map<String, String> serdeConfig = Collections.singletonMap(
        AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
    playEventSerde.configure(serdeConfig, false);

    final StreamsBuilder builder = new StreamsBuilder();
    builder.stream(PLAY_EVENTS, Consumed.with(Serdes.String(), playEventSerde))
        // group by key so we can count by session windows
        .groupByKey(Serialized.with(Serdes.String(), playEventSerde))
        // window by session
        .windowedBy(SessionWindows.with(INACTIVITY_GAP))
        // count play events per session
        .count(Materialized.<String, Long, SessionStore<Bytes, byte[]>>as(PLAY_EVENTS_PER_SESSION)
            .withKeySerde(Serdes.String())
            .withValueSerde(Serdes.Long()))
        // convert to a stream so we can map the key to a string
        .toStream()
        // map key to a readable string
        .map((key, value) -> new KeyValue<>(key.key() + "@" + key.window().start() + "->" + key.window().end(), value))
        // write to play-events-per-session topic
        .to(PLAY_EVENTS_PER_SESSION, Produced.with(Serdes.String(), Serdes.Long()));

    return new KafkaStreams(builder.build(), new StreamsConfig(config));
  }

}
