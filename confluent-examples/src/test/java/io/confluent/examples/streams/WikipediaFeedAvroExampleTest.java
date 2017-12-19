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
import io.confluent.examples.streams.kafka.EmbeddedSingleNodeKafkaCluster;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

public class WikipediaFeedAvroExampleTest {

  @ClassRule
  public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();
  private KafkaStreams streams;

  @BeforeClass
  public static void createTopics() {
    CLUSTER.createTopic(WikipediaFeedAvroExample.WIKIPEDIA_FEED);
    CLUSTER.createTopic(WikipediaFeedAvroExample.WIKIPEDIA_STATS);
  }

  @Before
  public void createStreams() throws IOException {
    streams =
        WikipediaFeedAvroExample.buildWikipediaFeed(CLUSTER.bootstrapServers(),
                                                    CLUSTER.schemaRegistryUrl(),
                                                    TestUtils.tempDirectory().getPath());
  }

  @After
  public void stopStreams() {
    streams.close();
  }

  @Test
  public void shouldRunTheWikipediaFeedExample() throws Exception {
    final Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
              StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
              io.confluent.kafka.serializers.KafkaAvroSerializer.class);
    props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, CLUSTER.schemaRegistryUrl());
    final KafkaProducer<String, WikiFeed> producer = new KafkaProducer<>(props);

    producer.send(new ProducerRecord<>(WikipediaFeedAvroExample.WIKIPEDIA_FEED,
                                       new WikiFeed("donna", true, "first post")));

    producer.send(new ProducerRecord<>(WikipediaFeedAvroExample.WIKIPEDIA_FEED,
                                       new WikiFeed("donna", true, "second post")));

    producer.send(new ProducerRecord<>(WikipediaFeedAvroExample.WIKIPEDIA_FEED,
                                       new WikiFeed("donna", true, "third post")));

    producer.send(new ProducerRecord<>(WikipediaFeedAvroExample.WIKIPEDIA_FEED,
                                       new WikiFeed("becca", true, "first post")));

    producer.send(new ProducerRecord<>(WikipediaFeedAvroExample.WIKIPEDIA_FEED,
                                       new WikiFeed("becca", true, "second post")));

    producer.send(new ProducerRecord<>(WikipediaFeedAvroExample.WIKIPEDIA_FEED,
                                       new WikiFeed("john", true, "first post")));
    producer.flush();

    streams.start();

    final Properties consumerProperties = new Properties();
    consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "wikipedia-feed-consumer");
    consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    final KafkaConsumer<String, Long>
        consumer =
        new KafkaConsumer<>(consumerProperties, new StringDeserializer(), new
            LongDeserializer());

    final Map<String, Long> expected = new HashMap<>();
    expected.put("donna", 3L);
    expected.put("becca", 2L);
    expected.put("john", 1L);

    final Map<String, Long> actual = new HashMap<>();

    consumer.subscribe(Collections.singleton(WikipediaFeedAvroExample.WIKIPEDIA_STATS));

    final long timeout = System.currentTimeMillis() + 30000L;
    while(!actual.equals(expected) && System.currentTimeMillis() < timeout) {
      final ConsumerRecords<String, Long> records = consumer.poll(1000);
      records.forEach(record -> actual.put(record.key(), record.value()));
    }

    assertThat(expected, equalTo(actual));

  }



}
