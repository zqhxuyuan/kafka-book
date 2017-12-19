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

import io.confluent.examples.streams.kafka.EmbeddedSingleNodeKafkaCluster;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.test.TestUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Demonstrates how to validate an application's expected state through interactive queries.
 *
 * Note: This example uses lambda expressions and thus works with Java 8+ only.
 */
public class ValidateStateWithInteractiveQueriesLambdaIntegrationTest {

  @ClassRule
  public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();

  private static String inputTopic = "inputTopic";

  @BeforeClass
  public static void startKafkaCluster() throws Exception {
    CLUSTER.createTopic(inputTopic);
  }

  @Test
  public void shouldComputeMaxValuePerKey() throws Exception {
    // A user may be listed multiple times.
    List<KeyValue<String, Long>> inputUserClicks = Arrays.asList(
        new KeyValue<>("alice", 13L),
        new KeyValue<>("bob", 4L),
        new KeyValue<>("chao", 25L),
        new KeyValue<>("bob", 19L),
        new KeyValue<>("chao", 56L),
        new KeyValue<>("alice", 78L),
        new KeyValue<>("alice", 40L),
        new KeyValue<>("bob", 3L)
    );

    Map<String, Long> expectedMaxClicksPerUser = new HashMap<String, Long>() {
      {
        put("alice", 78L);
        put("bob", 19L);
        put("chao", 56L);
      }
    };

    //
    // Step 1: Configure and start the processor topology.
    //
    StreamsBuilder builder = new StreamsBuilder();

    Properties streamsConfiguration = new Properties();
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "validating-with-interactive-queries-integration-test");
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
    streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    // The commit interval for flushing records to state stores and downstream must be lower than
    // this integration test's timeout (30 secs) to ensure we observe the expected processing results.
    streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 2 * 1000);
    streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    // Use a temporary directory for storing state, which will be automatically removed after the test.
    streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());

    KStream<String, Long> input = builder.stream(inputTopic);

    // rolling MAX() aggregation
    String maxStore = "max-store";
    input.groupByKey().aggregate(
        () -> Long.MIN_VALUE,
        (aggKey, value, aggregate) -> Math.max(value, aggregate),
        Materialized.as(maxStore)
    );

    // windowed MAX() aggregation
    String maxWindowStore = "max-window-store";
    input.groupByKey()
        .windowedBy(TimeWindows.of(TimeUnit.MINUTES.toMillis(1L)).until(TimeUnit.MINUTES.toMillis(5L)))
        .aggregate(
            () -> Long.MIN_VALUE,
            (aggKey, value, aggregate) -> Math.max(value, aggregate),
            Materialized.as(maxWindowStore));

    KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
    streams.start();

    //
    // Step 2: Produce some input data to the input topic.
    //
    Properties producerConfig = new Properties();
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
    producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
    producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
    IntegrationTestUtils.produceKeyValuesSynchronously(inputTopic, inputUserClicks, producerConfig);

    //
    // Step 3: Validate the application's state by interactively querying its state stores.
    //
    ReadOnlyKeyValueStore<String, Long> keyValueStore =
        IntegrationTestUtils.waitUntilStoreIsQueryable(maxStore, QueryableStoreTypes.keyValueStore(), streams);
    ReadOnlyWindowStore<String, Long> windowStore =
        IntegrationTestUtils.waitUntilStoreIsQueryable(maxWindowStore, QueryableStoreTypes.windowStore(), streams);

    // Wait a bit so that the input data can be fully processed to ensure that the stores can
    // actually be populated with data.  Running the build on (slow) Travis CI in particular
    // requires a few seconds to run this test reliably.
    Thread.sleep(3000);

    IntegrationTestUtils.assertThatKeyValueStoreContains(keyValueStore, expectedMaxClicksPerUser);
    IntegrationTestUtils.assertThatOldestWindowContains(windowStore, expectedMaxClicksPerUser);
    streams.close();
  }

}
