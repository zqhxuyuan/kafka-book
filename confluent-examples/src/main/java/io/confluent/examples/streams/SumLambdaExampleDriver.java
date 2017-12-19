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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;

import java.util.Collections;
import java.util.Properties;
import java.util.stream.IntStream;

/**
 * This is a sample driver for the {@link SumLambdaExample}.
 * To run this driver please first refer to the instructions in {@link SumLambdaExample}.
 * You can then run this class directly in your IDE or via the command line.
 * <p>
 * To run via the command line you might want to package as a fatjar first. Please refer to:
 * <a href='https://github.com/confluentinc/kafka-streams-examples#packaging-and-running'>Packaging</a>
 * <p>
 * Once packaged you can then run:
 * <pre>
 * {@code
 * $ java -cp target/kafka-streams-examples-4.0.0-SNAPSHOT-standalone.jar io.confluent.examples.streams.SumLambdaExampleDriver
 * }
 * </pre>
 * You should terminate with {@code Ctrl-C}.
 * Please refer to {@link SumLambdaExample} for instructions on running the example.
 * <p>
 * Note: This example uses lambda expressions and thus works with Java 8+ only.
 */
public class SumLambdaExampleDriver {

  public static void main(final String[] args) throws Exception {
    final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
    produceInput(bootstrapServers);
    consumeOutput(bootstrapServers);
  }

  private static void consumeOutput(String bootstrapServers) {
    final Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, "sum-lambda-example-consumer");
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    final KafkaConsumer<Integer, Integer> consumer = new KafkaConsumer<>(properties);
    consumer.subscribe(Collections.singleton(SumLambdaExample.SUM_OF_ODD_NUMBERS_TOPIC));
    while (true) {
      final ConsumerRecords<Integer, Integer> records =
              consumer.poll(Long.MAX_VALUE);

      for (final ConsumerRecord<Integer, Integer> record : records) {
        System.out.println("Current sum of odd numbers is:" + record.value());
      }
    }
  }

  private static void produceInput(String bootstrapServers) {
    final Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);

    final KafkaProducer<Integer, Integer> producer = new KafkaProducer<>(props);

    IntStream.range(0, 100)
            .mapToObj(val -> new ProducerRecord<>(SumLambdaExample.NUMBERS_TOPIC, val, val))
            .forEach(producer::send);

    producer.flush();
  }


}
