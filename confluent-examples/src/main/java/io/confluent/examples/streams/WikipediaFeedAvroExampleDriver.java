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

import io.confluent.examples.streams.avro.WikiFeed;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;
import java.util.stream.IntStream;

/**
 * This is a sample driver for the {@link WikipediaFeedAvroExample} and
 * To run this driver please first refer to the instructions in {@link WikipediaFeedAvroExample}
 * You can then run this class directly in your IDE or via the command line.
 *
 * To run via the command line you might want to package as a fatjar first. Please refer to:
 * <a href='https://github.com/confluentinc/kafka-streams-examples#packaging-and-running'>Packaging</a>
 *
 * Once packaged you can then run:
 * <pre>
 * {@code
 * java -cp target/kafka-streams-examples-4.0.0-SNAPSHOT-standalone.jar io.confluent.examples.streams.WikipediaFeedAvroExampleDriver
 * }
 * </pre>
 *
 * You should terminate with {@code Ctrl-C}.
 */
public class WikipediaFeedAvroExampleDriver {

  public static void main(final String[] args) throws IOException {
    final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
    final String schemaRegistryUrl = args.length > 1 ? args[1] : "http://localhost:8081";
    produceInputs(bootstrapServers, schemaRegistryUrl);
    consumeOutput(bootstrapServers, schemaRegistryUrl);
  }

  private static void produceInputs(String bootstrapServers, String schemaRegistryUrl) throws IOException {
    final String[] users = {"erica", "bob", "joe", "damian", "tania", "phil", "sam",
            "lauren", "joseph"};

    final Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            io.confluent.kafka.serializers.KafkaAvroSerializer.class);
    props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
    final KafkaProducer<String, WikiFeed> producer = new KafkaProducer<>(props);

    final Random random = new Random();

    IntStream.range(0, random.nextInt(100))
            .mapToObj(value -> new WikiFeed(users[random.nextInt(users.length)], true, "content"))
            .forEach(
                    record -> producer.send(new ProducerRecord<>(WikipediaFeedAvroExample.WIKIPEDIA_FEED, null, record)));

    producer.flush();
  }

  private static void consumeOutput(String bootstrapServers, String schemaRegistryUrl) {
    final Properties consumerProperties = new Properties();
    consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerProperties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
    consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "wikipedia-feed-example-consumer");
    consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    final KafkaConsumer<String, Long> consumer = new KafkaConsumer<>(consumerProperties,
            new StringDeserializer(),
            new LongDeserializer());

    consumer.subscribe(Collections.singleton(WikipediaFeedAvroExample.WIKIPEDIA_STATS));
    while (true) {
      final ConsumerRecords<String, Long> consumerRecords = consumer.poll(Long.MAX_VALUE);
      for (final ConsumerRecord<String, Long> consumerRecord : consumerRecords) {
        System.out.println(consumerRecord.key() + "=" + consumerRecord.value());
      }
    }
  }


}
