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
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end integration test that demonstrates how to handle corrupt input records (think: poison
 * pill messages) in a Kafka topic, which would normally lead to application failures due to
 * (de)serialization exceptions.
 *
 * In this example we choose to ignore/skip corrupted input records.  We describe further options at
 * http://docs.confluent.io/current/streams/faq.html, e.g. sending corrupted records to a quarantine
 * topic (think: dead letter queue).
 *
 * Note: This example uses lambda expressions and thus works with Java 8+ only.
 */
public class HandlingCorruptedInputRecordsIntegrationTest {

  @ClassRule
  public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();

  private static String inputTopic = "inputTopic";
  private static String outputTopic = "outputTopic";

  @BeforeClass
  public static void startKafkaCluster() throws Exception {
    CLUSTER.createTopic(inputTopic);
    CLUSTER.createTopic(outputTopic);
  }

  @Test
  public void shouldIgnoreCorruptInputRecords() throws Exception {
    List<Long> inputValues = Arrays.asList(1L, 2L, 3L);
    List<Long> expectedValues = inputValues.stream().map(x -> 2 * x).collect(Collectors.toList());

    //
    // Step 1: Configure and start the processor topology.
    //
    StreamsBuilder builder = new StreamsBuilder();

    Properties streamsConfiguration = new Properties();
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "failure-handling-integration-test");
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
    streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    Serde<String> stringSerde = Serdes.String();
    Serde<Long> longSerde = Serdes.Long();

    KStream<byte[], byte[]> input = builder.stream(inputTopic);

    // Note how the returned stream is of type `KStream<String, Long>`.
    KStream<String, Long> doubled = input.flatMap(
        (k, v) -> {
          try {
            // Attempt deserialization
            String key = stringSerde.deserializer().deserialize("input-topic", k);
            long value = longSerde.deserializer().deserialize("input-topic", v);

            // Ok, the record is valid (not corrupted).  Let's take the
            // opportunity to also process the record in some way so that
            // we haven't paid the deserialization cost just for "poison pill"
            // checking.
            return Collections.singletonList(KeyValue.pair(key, 2 * value));
          } catch (SerializationException e) {
            // Ignore/skip the corrupted record by catching the exception.
            // Optionally, we can log the fact that we did so:
            System.err.println("Could not deserialize record: " + e.getMessage());
          }
          return Collections.emptyList();
        }
    );

    // Write the processing results (which was generated from valid records only) to Kafka.
    doubled.to(outputTopic, Produced.with(stringSerde, longSerde));

    KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
    streams.start();

    //
    // Step 2: Produce some corrupt input data to the input topic.
    //
    Properties producerConfigForCorruptRecords = new Properties();
    producerConfigForCorruptRecords.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    producerConfigForCorruptRecords.put(ProducerConfig.ACKS_CONFIG, "all");
    producerConfigForCorruptRecords.put(ProducerConfig.RETRIES_CONFIG, 0);
    producerConfigForCorruptRecords.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    producerConfigForCorruptRecords.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    IntegrationTestUtils.produceValuesSynchronously(inputTopic,
        Collections.singletonList("corrupt"), producerConfigForCorruptRecords);

    //
    // Step 3: Produce some (valid) input data to the input topic.
    //
    Properties producerConfig = new Properties();
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
    producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
    producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
    IntegrationTestUtils.produceValuesSynchronously(inputTopic, inputValues, producerConfig);

    //
    // Step 4: Verify the application's output data.
    //
    Properties consumerConfig = new Properties();
    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "map-function-lambda-integration-test-standard-consumer");
    consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
    List<Long> actualValues = IntegrationTestUtils.waitUntilMinValuesRecordsReceived(consumerConfig,
        outputTopic, expectedValues.size());
    streams.close();
    assertThat(actualValues).isEqualTo(expectedValues);
  }
}
