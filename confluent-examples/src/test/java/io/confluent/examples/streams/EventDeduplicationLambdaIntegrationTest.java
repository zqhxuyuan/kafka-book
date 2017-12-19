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
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.test.TestUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end integration test that demonstrates how to remove duplicate records from an input
 * stream.
 *
 * Here, a stateful {@link org.apache.kafka.streams.kstream.Transformer} (from the Processor API)
 * detects and discards duplicate input records based on an "event id" that is embedded in each
 * input record.  This transformer is then included in a topology defined via the DSL.
 *
 * In this simplified example, the values of input records represent the event ID by which
 * duplicates will be detected.  In practice, record values would typically be a more complex data
 * structure, with perhaps one of the fields being such an event ID.  De-duplication by an event ID
 * is but one example of how to perform de-duplication in general.  The code example below can be
 * adapted to other de-duplication approaches.
 *
 * IMPORTANT:  Kafka including its Streams API support exactly-once semantics since version 0.11.
 * With this feature available, most use cases will no longer need to worry about duplicate messages
 * or duplicate processing.  That said, there will still be some use cases where you have your own
 * business rules that define when two events are considered to be "the same" and need to be
 * de-duplicated (e.g. two events having the same payload but different timestamps).  The example
 * below demonstrates how to implement your own business rules for event de-duplication.
 *
 * Note: This example uses lambda expressions and thus works with Java 8+ only.
 */
public class EventDeduplicationLambdaIntegrationTest {

  @ClassRule
  public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();

  private static String inputTopic = "inputTopic";
  private static String outputTopic = "outputTopic";

  private static String storeName = "eventId-store";

  @BeforeClass
  public static void startKafkaCluster() throws Exception {
    CLUSTER.createTopic(inputTopic);
    CLUSTER.createTopic(outputTopic);
  }

  /**
   * Discards duplicate records from the input stream.
   *
   * Duplicate records are detected based on an event ID;  in this simplified example, the record
   * value is the event ID.  The transformer remembers known event IDs in an associated window state
   * store, which automatically purges/expires event IDs from the store after a certain amount of
   * time has passed to prevent the store from growing indefinitely.
   *
   * Note: This code is for demonstration purposes and was not tested for production usage.
   */
  private static class DeduplicationTransformer<K, V, E> implements Transformer<K, V, KeyValue<K, V>> {

    private ProcessorContext context;

    /**
     * Key: event ID
     * Value: timestamp (event-time) of the corresponding event when the event ID was seen for the
     * first time
     */
    private WindowStore<E, Long> eventIdStore;

    private final long leftDurationMs;
    private final long rightDurationMs;

    private final KeyValueMapper<K, V, E> idExtractor;

    /**
     * @param maintainDurationPerEventInMs how long to "remember" a known event (or rather, an event
     *                                     ID), during the time of which any incoming duplicates of
     *                                     the event will be dropped, thereby de-duplicating the
     *                                     input.
     * @param idExtractor extracts a unique identifier from a record by which we de-duplicate input
     *                    records; if it returns null, the record will not be considered for
     *                    de-duping but forwarded as-is.
     */
    DeduplicationTransformer(long maintainDurationPerEventInMs, KeyValueMapper<K, V, E> idExtractor) {
      if (maintainDurationPerEventInMs < 1) {
        throw new IllegalArgumentException("maintain duration per event must be >= 1");
      }
      leftDurationMs = maintainDurationPerEventInMs / 2;
      rightDurationMs = maintainDurationPerEventInMs - leftDurationMs;
      this.idExtractor = idExtractor;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(final ProcessorContext context) {
      this.context = context;
      eventIdStore = (WindowStore<E, Long>) context.getStateStore(storeName);
    }

    public KeyValue<K, V> transform(final K key, final V value) {
      E eventId = idExtractor.apply(key, value);
      if (eventId == null) {
        return KeyValue.pair(key, value);
      } else {
        KeyValue<K, V> output;
        if (isDuplicate(eventId)) {
          output = null;
          updateTimestampOfExistingEventToPreventExpiry(eventId, context.timestamp());
        } else {
          output = KeyValue.pair(key, value);
          rememberNewEvent(eventId, context.timestamp());
        }
        return output;
      }
    }

    private boolean isDuplicate(final E eventId) {
      long eventTime = context.timestamp();
      WindowStoreIterator<Long> timeIterator = eventIdStore.fetch(
          eventId,
          eventTime - leftDurationMs,
          eventTime + rightDurationMs);
      boolean isDuplicate = timeIterator.hasNext();
      timeIterator.close();
      return isDuplicate;
    }

    private void updateTimestampOfExistingEventToPreventExpiry(final E eventId, long newTimestamp) {
      eventIdStore.put(eventId, newTimestamp, newTimestamp);
    }

    private void rememberNewEvent(final E eventId, long timestamp) {
      eventIdStore.put(eventId, timestamp, timestamp);
    }

    @Override
    public KeyValue<K, V> punctuate(final long timestamp) {
      // our windowStore segments are closed automatically
      return null;
    }

    @Override
    public void close() {
      // Note: The store should NOT be closed manually here via `eventIdStore.close()`!
      // The Kafka Streams API will automatically close stores when necessary.
    }

  }

  @Test
  public void shouldRemoveDuplicatesFromTheInput() throws Exception {
    String firstId = UUID.randomUUID().toString(); // e.g. "4ff3cb44-abcb-46e3-8f9a-afb7cc74fbb8"
    String secondId = UUID.randomUUID().toString();
    String thirdId = UUID.randomUUID().toString();
    List<String> inputValues = Arrays.asList(firstId, secondId, firstId, firstId, secondId, thirdId,
        thirdId, firstId, secondId);
    List<String> expectedValues = Arrays.asList(firstId, secondId, thirdId);

    //
    // Step 1: Configure and start the processor topology.
    //
    StreamsBuilder builder = new StreamsBuilder();

    Properties streamsConfiguration = new Properties();
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "deduplication-lambda-integration-test");
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    // The commit interval for flushing records to state stores and downstream must be lower than
    // this integration test's timeout (30 secs) to ensure we observe the expected processing results.
    streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, TimeUnit.SECONDS.toMillis(10));
    streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    // Use a temporary directory for storing state, which will be automatically removed after the test.
    streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());

    // How long we "remember" an event.  During this time, any incoming duplicates of the event
    // will be, well, dropped, thereby de-duplicating the input data.
    //
    // The actual value depends on your use case.  To reduce memory and disk usage, you could
    // decrease the size to purge old windows more frequently at the cost of potentially missing out
    // on de-duplicating late-arriving records.
    long maintainDurationPerEventInMs = TimeUnit.MINUTES.toMillis(10);

    // The number of segments has no impact on "correctness".
    // Using more segments implies larger overhead but allows for more fined grained record expiration
    // Note: the specified retention time is a _minimum_ time span and no strict upper time bound
    int numberOfSegments = 3;

    // retention period must be at least window size -- for this use case, we don't need a longer retention period
    // and thus just use the window size as retention time
    long retentionPeriod = maintainDurationPerEventInMs;

    StoreBuilder<WindowStore<String, Long>> dedupStoreBuilder = Stores.windowStoreBuilder(
            Stores.persistentWindowStore(storeName,
                                         retentionPeriod,
                                         numberOfSegments,
                                         maintainDurationPerEventInMs,
                                         false
            ),
            Serdes.String(),
            Serdes.Long());


    builder.addStateStore(dedupStoreBuilder);

    KStream<byte[], String> input = builder.stream(inputTopic);
    KStream<byte[], String> deduplicated = input.transform(
        // In this example, we assume that the record value as-is represents a unique event ID by
        // which we can perform de-duplication.  If your records are different, adapt the extractor
        // function as needed.
        () -> new DeduplicationTransformer<>(maintainDurationPerEventInMs, (key, value) -> value),
        storeName);
    deduplicated.to(outputTopic);

    KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
    streams.start();

    //
    // Step 2: Produce some input data to the input topic.
    //
    Properties producerConfig = new Properties();
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
    producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
    producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    IntegrationTestUtils.produceValuesSynchronously(inputTopic, inputValues, producerConfig);

    //
    // Step 3: Verify the application's output data.
    //
    Properties consumerConfig = new Properties();
    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "deduplication-integration-test-standard-consumer");
    consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    List<String> actualValues = IntegrationTestUtils.waitUntilMinValuesRecordsReceived(consumerConfig,
        outputTopic, expectedValues.size());
    streams.close();
    assertThat(actualValues).containsExactlyElementsOf(expectedValues);
  }

}
