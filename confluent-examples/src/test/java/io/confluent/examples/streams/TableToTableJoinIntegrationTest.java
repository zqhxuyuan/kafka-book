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
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.test.TestUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end integration test that demonstrates how to perform a join between two KTables.
 * We also show how to assign a state store to the joined table, which is required to make it
 * accessible for interactive queries.
 *
 * Note: This example uses lambda expressions and thus works with Java 8+ only.
 */
public class TableToTableJoinIntegrationTest {

  @ClassRule
  public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();

  private static final String userRegionTopic = "user-region-topic";
  private static final String userLastLoginTopic = "user-last-login-topic";
  private static final String outputTopic = "output-topic";

  @BeforeClass
  public static void startKafkaCluster() throws Exception {
    CLUSTER.createTopic(userRegionTopic);
    CLUSTER.createTopic(userLastLoginTopic);
    CLUSTER.createTopic(outputTopic);
  }

  @Test
  public void shouldJoinTwoTables() throws Exception {
    // Input: Region per user (multiple records allowed per user).
    List<KeyValue<String, String>> userRegionRecords = Arrays.asList(
        new KeyValue<>("alice", "asia"),
        new KeyValue<>("bob", "europe"),
        new KeyValue<>("alice", "europe"),
        new KeyValue<>("charlie", "europe"),
        new KeyValue<>("bob", "asia")
    );

    // Input 2: Timestamp of last login per user (multiple records allowed per user)
    List<KeyValue<String, Long>> userLastLoginRecords = Arrays.asList(
        new KeyValue<>("alice", 1485500000L),
        new KeyValue<>("bob", 1485520000L),
        new KeyValue<>("alice", 1485530000L),
        new KeyValue<>("bob", 1485560000L)
    );

    List<KeyValue<String, String>> expectedResults = Arrays.asList(
        new KeyValue<>("alice", "europe/1485500000"),
        new KeyValue<>("bob", "asia/1485520000"),
        new KeyValue<>("alice", "europe/1485530000"),
        new KeyValue<>("bob", "asia/1485560000")
    );

    List<KeyValue<String, String>> expectedResultsForJoinStateStore = Arrays.asList(
        new KeyValue<>("alice", "europe/1485530000"),
        new KeyValue<>("bob", "asia/1485560000")
    );

    //
    // Step 1: Configure and start the processor topology.
    //
    final Serde<String> stringSerde = Serdes.String();
    final Serde<Long> longSerde = Serdes.Long();

    Properties streamsConfiguration = new Properties();
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "table-table-join-lambda-integration-test");
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    // For didactic reasons: disable record caching so we can observe every individual update record being sent downstream
    streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
    // The commit interval for flushing records to state stores and downstream must be lower than
    // this integration test's timeout (30 secs) to ensure we observe the expected processing results.
    streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
    streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    // Use a temporary directory for storing state, which will be automatically removed after the test.
    streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());

    StreamsBuilder builder = new StreamsBuilder();
    KTable<String, String> userRegions = builder.table(userRegionTopic);
    KTable<String, Long> userLastLogins = builder.table(userLastLoginTopic, Consumed.with(stringSerde, longSerde));

    String storeName = "joined-store";
    userRegions.join(userLastLogins,
        (regionValue, lastLoginValue) -> regionValue + "/" + lastLoginValue,
        Materialized.as(storeName))
        .toStream()
        .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));


    KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
    streams.start();

    //
    // Step 2: Publish user regions.
    //
    Properties regionsProducerConfig = new Properties();
    regionsProducerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    regionsProducerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
    regionsProducerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
    regionsProducerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    regionsProducerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    IntegrationTestUtils.produceKeyValuesSynchronously(userRegionTopic, userRegionRecords, regionsProducerConfig);

    //
    // Step 3: Publish user's last login timestamps.
    //
    Properties lastLoginProducerConfig = new Properties();
    lastLoginProducerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    lastLoginProducerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
    lastLoginProducerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
    lastLoginProducerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    lastLoginProducerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
    IntegrationTestUtils.produceKeyValuesSynchronously(userLastLoginTopic, userLastLoginRecords, lastLoginProducerConfig);

    //
    // Step 4: Verify the application's output data.
    //
    Properties consumerConfig = new Properties();
    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "stream-stream-join-lambda-integration-test-standard-consumer");
    consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    List<KeyValue<String, String>> actualResults = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig,
        outputTopic, expectedResults.size());

    // Verify the (local) state store of the joined table.
    // For a comprehensive demonstration of interactive queries please refer to KafkaMusicExample.
    ReadOnlyKeyValueStore<String, String> readOnlyKeyValueStore =
        streams.store(storeName, QueryableStoreTypes.keyValueStore());
    KeyValueIterator<String, String> keyValueIterator = readOnlyKeyValueStore.all();
    assertThat(keyValueIterator).containsExactlyElementsOf(expectedResultsForJoinStateStore);

    streams.close();
    assertThat(actualResults).containsExactlyElementsOf(expectedResults);
  }

}
