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
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end integration test that demonstrates how to mix and match the DSL and the Processor
 * API of Kafka Streams.
 *
 * More concretely, we show how to use the {@link KStream#transform(TransformerSupplier, String...)}
 * method to include a custom {@link org.apache.kafka.streams.kstream.Transformer} (from the
 * Processor API) in a topology defined via the DSL.  The fictitious use case is to anonymize
 * IPv4 addresses contained in the input data.
 *
 * Tip: Users that want to use {@link KStream#process(ProcessorSupplier, String...)} would need to
 * include a custom {@link org.apache.kafka.streams.processor.Processor}.  Keep in mind though that
 * the return type of {@link KStream#process(ProcessorSupplier, String...)} is `void`, which means
 * you cannot add further operators (such as `to()` as we do below) after having called `process()`.
 * If you need to add further operators, you'd have to use `transform()` as we do in this example.
 *
 * Note: This example uses lambda expressions and thus works with Java 8+ only.
 */
public class MixAndMatchLambdaIntegrationTest {

  @ClassRule
  public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();

  private static String inputTopic = "inputTopic";
  private static String outputTopic = "outputTopic";

  @BeforeClass
  public static void startKafkaCluster() throws Exception {
    CLUSTER.createTopic(inputTopic);
    CLUSTER.createTopic(outputTopic);
  }

  /**
   * Performs rudimentary anonymization of IPv4 address in the input data.
   * You should use this for demonstration purposes only.
   */
  private static class AnonymizeIpAddressTransformer implements Transformer<byte[], String, KeyValue<byte[], String>> {

    private static Pattern ipv4AddressPattern =
        Pattern.compile("(?<keep>[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.)(?<anonymize>[0-9]{1,3})");

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
      // Not needed.
    }

    @Override
    public KeyValue<byte[], String> transform(final byte[] recordKey, final String recordValue) {
      // The record value contains the IP address in string representation.
      // The original record key is ignored because we don't need it for this logic.
      String anonymizedIpAddress = anonymizeIpAddress(recordValue);
      return KeyValue.pair(recordKey, anonymizedIpAddress);
    }

    /**
     * Anonymizes the provided IPv4 address (in string representation) by replacing the fourth byte
     * with "XXX".  For example, "192.168.1.234" is anonymized to "192.168.1.XXX".
     *
     * Note: This method is for illustration purposes only.  The anonymization is both lackluster
     * (in terms of the achieved anonymization) and slow/inefficient (in terms of implementation).
     *
     * @param ipAddress The IPv4 address
     * @return Anonymized IPv4 address.
     */
    private String anonymizeIpAddress(String ipAddress) {
      return ipv4AddressPattern.matcher(ipAddress).replaceAll("${keep}XXX");
    }

    @Override
    public KeyValue<byte[], String> punctuate(long timestamp) {
      // We don't need any periodic actions in this transformer.  Returning null achieves that.
      return null;
    }

    @Override
    public void close() {
      // Not needed.
    }

  }


  @Test
  public void shouldAnonymizeTheInput() throws Exception {
    List<String> inputValues = Arrays.asList("Hello, 1.2.3.4!", "foo 192.168.1.55 bar");
    List<String> expectedValues = Arrays.asList("HELLO, 1.2.3.XXX!", "FOO 192.168.1.XXX BAR");

    //
    // Step 1: Configure and start the processor topology.
    //
    StreamsBuilder builder = new StreamsBuilder();

    Properties streamsConfiguration = new Properties();
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "mix-and-match-lambda-integration-test");
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    KStream<byte[], String> input = builder.stream(inputTopic);
    KStream<byte[], String> uppercasedAndAnonymized = input
        .mapValues(String::toUpperCase)
        .transform(AnonymizeIpAddressTransformer::new);
    uppercasedAndAnonymized.to(outputTopic);

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
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "mix-and-match-lambda-integration-test-standard-consumer");
    consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    List<String> actualValues = IntegrationTestUtils.waitUntilMinValuesRecordsReceived(consumerConfig,
        outputTopic, expectedValues.size());
    streams.close();
    assertThat(actualValues).isEqualTo(expectedValues);
  }

}
