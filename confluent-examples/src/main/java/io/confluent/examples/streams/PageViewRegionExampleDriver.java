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

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
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
import java.io.InputStream;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;

/**
 * This is a sample driver for the {@link PageViewRegionExample} and {@link PageViewRegionLambdaExample}.
 * To run this driver please first refer to the instructions in {@link PageViewRegionExample} or {@link PageViewRegionLambdaExample}.
 * You can then run this class directly in your IDE or via the command line.
 * <p>
 * To run via the command line you might want to package as a fatjar first. Please refer to:
 * <a href='https://github.com/confluentinc/kafka-streams-examples#packaging-and-running'>Packaging</a>
 * <p>
 * Once packaged you can then run:
 * <pre>
 * {@code
 * $ java -cp target/kafka-streams-examples-4.0.0-SNAPSHOT-standalone.jar io.confluent.examples.streams.PageViewRegionExampleDriver
 * }
 * </pre>
 * You should terminate with {@code Ctrl-C}.
 */
public class PageViewRegionExampleDriver {

  public static void main(final String[] args) throws IOException {
    final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
    final String schemaRegistryUrl = args.length > 1 ? args[1] : "http://localhost:8081";
    produceInputs(bootstrapServers, schemaRegistryUrl);
    consumeOutput(bootstrapServers);
  }

  private static void produceInputs(String bootstrapServers, String schemaRegistryUrl) throws IOException {
    final String[] users = {"erica", "bob", "joe", "damian", "tania", "phil", "sam", "lauren", "joseph"};
    final String[] regions = {"europe", "usa", "asia", "africa"};

    final Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
    props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

    final GenericRecordBuilder pageViewBuilder =
      new GenericRecordBuilder(loadSchema("pageview.avsc"));
    final GenericRecordBuilder userProfileBuilder =
      new GenericRecordBuilder(loadSchema("userprofile.avsc"));

    final String pageViewsTopic = "PageViews";
    final String userProfilesTopic = "UserProfiles";

    final Random random = new Random();
    pageViewBuilder.set("industry", "eng");
    try (final KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(props)) {
      for (final String user : users) {
        userProfileBuilder.set("experience", "some");
        userProfileBuilder.set("region", regions[random.nextInt(regions.length)]);
        producer.send(new ProducerRecord<>(userProfilesTopic, user, userProfileBuilder.build()));
        // For each user generate some page views
        for (int i=0; i<random.nextInt(10); i++) {
          pageViewBuilder.set("user", user);
          pageViewBuilder.set("page", "index.html");
          final GenericData.Record record = pageViewBuilder.build();
          producer.send(new ProducerRecord<>(pageViewsTopic, null, record));
        }
      }
    }
  }

  private static void consumeOutput(String bootstrapServers) {
    final String resultTopic = "PageViewsByRegion";
    final Properties consumerProperties = new Properties();
    consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
    consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG,
      "pageview-region-lambda-example-consumer");
    consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    try (final KafkaConsumer<String, Long> consumer = new KafkaConsumer<>(consumerProperties)) {
      consumer.subscribe(Collections.singleton(resultTopic));
      while (true) {
        final ConsumerRecords<String, Long> consumerRecords = consumer.poll(Long.MAX_VALUE);
        for (final ConsumerRecord<String, Long> consumerRecord : consumerRecords) {
          System.out.println(consumerRecord.key() + ":" + consumerRecord.value());
        }
      }
    }
  }

  private static Schema loadSchema(final String name) throws IOException {
    try (InputStream input = PageViewRegionLambdaExample.class.getClassLoader()
      .getResourceAsStream("avro/io/confluent/examples/streams/" + name)) {
      return new Schema.Parser().parse(input);
    }
  }

}
