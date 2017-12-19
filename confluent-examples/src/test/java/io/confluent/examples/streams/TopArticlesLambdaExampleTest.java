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
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.WindowedDeserializer;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import static io.confluent.examples.streams.TopArticlesExampleDriver.loadSchema;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

public class TopArticlesLambdaExampleTest {

  @ClassRule
  public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();
  private final String [] users = {"jo", "lauren", "tim", "sam"};
  private final Random random = new Random();
  private KafkaStreams streams;

  @BeforeClass
  public static void createTopics() {
    CLUSTER.createTopic(TopArticlesLambdaExample.TOP_NEWS_PER_INDUSTRY_TOPIC);
    CLUSTER.createTopic(TopArticlesLambdaExample.PAGE_VIEWS);
  }

  @Before
  public void createStreams() throws IOException {
    streams =
        TopArticlesLambdaExample.buildTopArticlesStream(CLUSTER.bootstrapServers(),
                                                        CLUSTER.schemaRegistryUrl(),
                                                        TestUtils.tempDirectory().getPath());
  }

  @After
  public void stopStreams() {
    streams.close();
  }

  @Test
  public void shouldProduceTopNArticles() throws Exception {
    final Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
              StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
              io.confluent.kafka.serializers.KafkaAvroSerializer.class);
    props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, CLUSTER.schemaRegistryUrl());
    final KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(props);

    final GenericRecordBuilder pageViewBuilder =
        new GenericRecordBuilder(loadSchema("pageview.avsc"));

    pageViewBuilder.set("flags", "ARTICLE");

    pageViewBuilder.set("industry", "eng");
    createPageViews(producer, pageViewBuilder, 3, "news");
    createPageViews(producer, pageViewBuilder, 2, "random");
    createPageViews(producer, pageViewBuilder, 1, "stuff");

    pageViewBuilder.set("industry", "science");
    createPageViews(producer, pageViewBuilder, 3, "index");
    createPageViews(producer, pageViewBuilder, 2, "about");
    createPageViews(producer, pageViewBuilder, 1, "help");

    final Map<String, List<String>> expected = new HashMap<>();
    expected.put("eng", Arrays.asList("news", "random", "stuff"));
    expected.put("science", Arrays.asList("index", "about", "help"));

    streams.start();

    final Properties consumerProperties = new Properties();
    consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "top-articles-consumer");
    consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    final Deserializer<Windowed<String>>
        windowedDeserializer = new WindowedDeserializer<>(Serdes.String().deserializer());
    final KafkaConsumer<Windowed<String>, String> consumer = new KafkaConsumer<>(consumerProperties,
                                                                                 windowedDeserializer,
                                                                                 Serdes.String().deserializer());

    consumer.subscribe(Collections.singletonList(TopArticlesLambdaExample.TOP_NEWS_PER_INDUSTRY_TOPIC));

    final Map<String, List<String>> received = new HashMap<>();
    final long timeout = System.currentTimeMillis() + 30000L;
    boolean done = false;
    while(System.currentTimeMillis() < timeout && !done) {
      final ConsumerRecords<Windowed<String>, String> records = consumer.poll(10);
      records.forEach(record ->
                          received.put(record.key().key(), Arrays.asList(record.value().split("\n"))));

      done = received.equals(expected);
    }

    assertThat(received, equalTo(expected));

  }

  private void createPageViews(final KafkaProducer<String, GenericRecord> producer,
                               final GenericRecordBuilder pageViewBuilder,
                               final int count, final String page) {
    pageViewBuilder.set("page", page);
    for (int i = 0; i< count; i++) {
      pageViewBuilder.set("user", users[random.nextInt(users.length)]);
      producer.send(new ProducerRecord<>(TopArticlesLambdaExample.PAGE_VIEWS,
                                         pageViewBuilder.build()));
    }
    producer.flush();
  }

}