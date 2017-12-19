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
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

/**
 * This is a sample driver for the {@link SessionWindowsExample}.
 * To run this driver please first refer to the instructions in {@link SessionWindowsExample}.
 * You can then run this class directly in your IDE or via the command line.
 * <p>
 * To run via the command line you might want to package as a fatjar first. Please refer to:
 * <a href='https://github.com/confluentinc/kafka-streams-examples#packaging-and-running'>Packaging</a>
 * <p>
 * Once packaged you can then run:
 * <pre>
 * {@code
 * $ java -cp target/kafka-streams-examples-4.0.0-SNAPSHOT-standalone.jar io.confluent.examples.streams.SessionWindowsExampleDriver
 * }
 * </pre>
 */
public class SessionWindowsExampleDriver {

  public static final int NUM_RECORDS_SENT = 8;

  public static void main(String[] args) {
    final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
    final String schemaRegistryUrl = args.length > 1 ? args[1] : "http://localhost:8081";
    producePlayEvents(bootstrapServers, schemaRegistryUrl);
    consumeOutput(bootstrapServers);
  }

  private static void producePlayEvents(final String bootstrapServers, final String schemaRegistryUrl) {

    final SpecificAvroSerializer<PlayEvent> playEventSerializer = new SpecificAvroSerializer<>();
    final Map<String, String> serdeConfig = Collections.singletonMap(
        AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
    playEventSerializer.configure(serdeConfig, false);

    final Properties producerProperties = new Properties();
    producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

    final KafkaProducer<String, PlayEvent> playEventProducer = new KafkaProducer<>(producerProperties,
                                                                                   Serdes.String() .serializer(),
                                                                                   playEventSerializer);

    final long start = System.currentTimeMillis();
    final long billEvenTime = start + SessionWindowsExample.INACTIVITY_GAP / 10;
    // create three sessions with different times
    playEventProducer.send(new ProducerRecord<>(SessionWindowsExample.PLAY_EVENTS,
                                                null,
                                                start,
                                                "jo",
                                                new PlayEvent(1L, 10L)));

    playEventProducer.send(new ProducerRecord<>(SessionWindowsExample.PLAY_EVENTS,
                                                null,
                                                billEvenTime,
                                                "bill",
                                                new PlayEvent(2L, 10L)));
    playEventProducer.send(new ProducerRecord<>(SessionWindowsExample.PLAY_EVENTS,
                                                null,
                                                start + SessionWindowsExample.INACTIVITY_GAP / 5,
                                                "sarah",
                                                new PlayEvent(2L, 10L)));

    // out-of-order event for jo that is outside inactivity gap so will create a new session
    playEventProducer.send(new ProducerRecord<>(SessionWindowsExample.PLAY_EVENTS,
                                                null,
                                                start + SessionWindowsExample.INACTIVITY_GAP + 1,
                                                "jo",
                                                new PlayEvent(1L, 10L)));
    // extend current session for bill
    playEventProducer.send(new ProducerRecord<>(SessionWindowsExample.PLAY_EVENTS,
                                                null,
                                                start + SessionWindowsExample.INACTIVITY_GAP,
                                                "bill",
                                                new PlayEvent(2L, 10L)));

    // new session for sarah
    playEventProducer.send(new ProducerRecord<>(SessionWindowsExample.PLAY_EVENTS,
                                                null,
                                                start + 2 * SessionWindowsExample.INACTIVITY_GAP,
                                                "sarah",
                                                new PlayEvent(2L, 10L)));

    // send earlier event for jo that will merge the 2 previous sessions
    playEventProducer.send(new ProducerRecord<>(SessionWindowsExample.PLAY_EVENTS,
                                                null,
                                                start + SessionWindowsExample.INACTIVITY_GAP / 2,
                                                "jo",
                                                new PlayEvent(1L, 10L)));

    // new session for bill
    playEventProducer.send(new ProducerRecord<>(SessionWindowsExample.PLAY_EVENTS,
                                                null,
                                                start + 3 * SessionWindowsExample.INACTIVITY_GAP,
                                                "bill",
                                                new PlayEvent(2L, 10L)));

    // extend session session for sarah
    // new session for sarah
    playEventProducer.send(new ProducerRecord<>(SessionWindowsExample.PLAY_EVENTS,
                                                null,
                                                start + 2 * SessionWindowsExample.INACTIVITY_GAP +
                                                SessionWindowsExample.INACTIVITY_GAP / 5,
                                                "sarah",
                                                new PlayEvent(2L, 10L)));

    playEventProducer.close();
  }

  private static void consumeOutput(final String bootstrapServers) {
    final Properties consumerProps = new Properties();
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "session-windows-consumer");
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Serdes.String().deserializer().getClass());
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Serdes.Long().deserializer().getClass());

    final KafkaConsumer<String, Long> consumer = new KafkaConsumer<>(consumerProps);
    consumer.subscribe(Collections.singleton(SessionWindowsExample.PLAY_EVENTS_PER_SESSION));
    int received = 0;
    while(received < NUM_RECORDS_SENT) {
      final ConsumerRecords<String, Long> records = consumer.poll(Long.MAX_VALUE);
      records.forEach(record -> System.out.println(record.key() + " = " + record.value()));
      received += records.count();
    }

    consumer.close();
  }
}
