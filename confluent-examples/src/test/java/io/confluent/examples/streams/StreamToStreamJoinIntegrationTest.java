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
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.test.TestUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end integration test that demonstrates how to perform a join between two KStreams.
 *
 * Note: This example uses lambda expressions and thus works with Java 8+ only.
 */
public class StreamToStreamJoinIntegrationTest {

  @ClassRule
  public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();

  private static final String adImpressionsTopic = "adImpressions";
  private static final String adClicksTopic = "adClicks";
  private static final String outputTopic = "output-topic";

  @BeforeClass
  public static void startKafkaCluster() throws Exception {
    CLUSTER.createTopic(adImpressionsTopic);
    CLUSTER.createTopic(adClicksTopic);
    CLUSTER.createTopic(outputTopic);
  }

  @Test
  public void shouldJoinTwoStreams() throws Exception {
    // Input 1: Ad impressions
    List<KeyValue<String, String>> inputAdImpressions = Arrays.asList(
        new KeyValue<>("car-advertisement", "shown"),
        new KeyValue<>("newspaper-advertisement", "shown"),
        new KeyValue<>("gadget-advertisement", "shown")
    );

    // Input 2: Ad clicks
    List<KeyValue<String, String>> inputAdClicks = Arrays.asList(
        new KeyValue<>("newspaper-advertisement", "clicked"),
        new KeyValue<>("gadget-advertisement", "clicked"),
        new KeyValue<>("newspaper-advertisement", "clicked")
    );

    List<KeyValue<String, String>> expectedResults = Arrays.asList(
        new KeyValue<>("car-advertisement", "shown/null"),
        new KeyValue<>("newspaper-advertisement", "shown/null"),
        new KeyValue<>("gadget-advertisement", "shown/null"),
        new KeyValue<>("newspaper-advertisement", "shown/clicked"),
        new KeyValue<>("gadget-advertisement", "shown/clicked"),
        new KeyValue<>("newspaper-advertisement", "shown/clicked")
    );

    //
    // Step 1: Configure and start the processor topology.
    //
    final Serde<String> stringSerde = Serdes.String();

    Properties streamsConfiguration = new Properties();
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-stream-join-lambda-integration-test");
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    // The commit interval for flushing records to state stores and downstream must be lower than
    // this integration test's timeout (30 secs) to ensure we observe the expected processing results.
    streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
    streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    // Use a temporary directory for storing state, which will be automatically removed after the test.
    streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());

    StreamsBuilder builder = new StreamsBuilder();
    KStream<String, String> alerts = builder.stream(adImpressionsTopic);
    KStream<String, String> incidents = builder.stream(adClicksTopic);

    // In this example, we opt to perform an OUTER JOIN between the two streams.  We picked this
    // join type to show how the Streams API will send further join updates downstream whenever,
    // for the same join key (e.g. "newspaper-advertisement"), we receive an update from either of
    // the two joined streams during the defined join window.
    KStream<String, String> impressionsAndClicks = alerts.outerJoin(incidents,
        (impressionValue, clickValue) -> impressionValue + "/" + clickValue,
        // KStream-KStream joins are always windowed joins, hence we must provide a join window.
        JoinWindows.of(TimeUnit.SECONDS.toMillis(5)));

    // Write the results to the output topic.
    impressionsAndClicks.to(outputTopic);

    KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
    streams.start();

    //
    // Step 2: Publish ad impressions.
    //
    Properties alertsProducerConfig = new Properties();
    alertsProducerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    alertsProducerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
    alertsProducerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
    alertsProducerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    alertsProducerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    IntegrationTestUtils.produceKeyValuesSynchronously(adImpressionsTopic, inputAdImpressions, alertsProducerConfig);

    //
    // Step 3: Publish ad clicks.
    //
    Properties incidentsProducerConfig = new Properties();
    incidentsProducerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    incidentsProducerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
    incidentsProducerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
    incidentsProducerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    incidentsProducerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    IntegrationTestUtils.produceKeyValuesSynchronously(adClicksTopic, inputAdClicks, incidentsProducerConfig);

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
    streams.close();
    assertThat(actualResults).containsExactlyElementsOf(expectedResults);
  }

}
