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
import io.confluent.examples.streams.kafka.EmbeddedSingleNodeKafkaCluster;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlySessionStore;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class SessionWindowsExampleTest {

  @ClassRule
  public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();
  private KafkaStreams streams;

  @BeforeClass
  public static void createTopics() {
    CLUSTER.createTopic(SessionWindowsExample.PLAY_EVENTS);
    CLUSTER.createTopic(SessionWindowsExample.PLAY_EVENTS_PER_SESSION);
  }

  @Before
  public void createStreams() {
    streams =
        SessionWindowsExample.createStreams(CLUSTER.bootstrapServers(),
                                            CLUSTER.schemaRegistryUrl(),
                                            TestUtils.tempDirectory().getPath());
    streams.start();
  }

  @After
  public void closeStreams() {
    streams.close();
  }

  @Test
  public void shouldCountPlayEventsBySession() throws Exception {
    final Map<String, String> serdeConfig = Collections.singletonMap(
        AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, CLUSTER.schemaRegistryUrl());
    final SpecificAvroSerializer<PlayEvent> playEventSerializer = new SpecificAvroSerializer<>();
    playEventSerializer.configure(serdeConfig, false);

    final Properties producerProperties = new Properties();
    producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());

    final KafkaProducer<String, PlayEvent> playEventProducer = new KafkaProducer<>(producerProperties,
                                                                                   Serdes.String() .serializer(),
                                                                                   playEventSerializer);

    final Properties consumerProps = new Properties();
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "session-windows-consumer");
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Serdes.String().deserializer().getClass());
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Serdes.Long().deserializer().getClass());


    final long start = System.currentTimeMillis();

    final String userId = "erica";
    playEventProducer.send(new ProducerRecord<>(SessionWindowsExample.PLAY_EVENTS,
                                                null,
                                                start,
                                                userId,
                                                new PlayEvent(1L, 10L)));

    final List<KeyValue<String, Long>>
        firstSession =
        IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerProps,
                                                                 SessionWindowsExample.PLAY_EVENTS_PER_SESSION,
                                                                 1);

    // should have a session for erica with start and end time the same
    assertThat(firstSession.get(0), equalTo(KeyValue.pair(userId + "@" +start+"->"+start, 1L)));

    // also look in the store to find the same session
    final ReadOnlySessionStore<String, Long>
        playEventsPerSession =
        streams.store(SessionWindowsExample.PLAY_EVENTS_PER_SESSION, QueryableStoreTypes.<String, Long>sessionStore());

    final KeyValue<Windowed<String>, Long> next = fetchSessionsFromLocalStore(userId, playEventsPerSession).get(0);
    assertThat(next.key, equalTo(new Windowed<>(userId, new SessionWindow(start, start))));
    assertThat(next.value, equalTo(1L));

    // send another event that is after the inactivity gap, so we have 2 independent sessions
    final long secondSessionStart = start + SessionWindowsExample.INACTIVITY_GAP + 1;
    playEventProducer.send(new ProducerRecord<>(SessionWindowsExample.PLAY_EVENTS,
                                                null,
                                                secondSessionStart,
                                                userId,
                                                new PlayEvent(2L, 10L)));

    final List<KeyValue<String, Long>>
        secondSession =
        IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerProps,
                                                                 SessionWindowsExample.PLAY_EVENTS_PER_SESSION,
                                                                 1);
    // should have created a new session
    assertThat(secondSession.get(0), equalTo(KeyValue.pair(userId + "@" + secondSessionStart + "->" + secondSessionStart,
                                                           1L)));

    // should now have 2 active sessions in the store
    final List<KeyValue<Windowed<String>, Long>> results = fetchSessionsFromLocalStore(userId, playEventsPerSession);
    assertThat(results, equalTo(Arrays.asList(KeyValue.pair(new Windowed<>(userId, new SessionWindow(start, start)),1L),
                                              KeyValue.pair(new Windowed<>(userId, new SessionWindow(secondSessionStart, secondSessionStart)),1L))));

    // create an event between the two sessions to demonstrate merging
    final long mergeTime = start + SessionWindowsExample.INACTIVITY_GAP / 2;
    playEventProducer.send(new ProducerRecord<>(SessionWindowsExample.PLAY_EVENTS,
                                                null,
                                                mergeTime,
                                                userId,
                                                new PlayEvent(3L, 10L)));

    playEventProducer.close();


    final List<KeyValue<String, Long>>
        merged =
        IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerProps,
                                                                 SessionWindowsExample.PLAY_EVENTS_PER_SESSION,
                                                                 3);
    // should have merged all sessions into one and sent tombstones for the sessions that were
    // merged
    assertThat(merged, equalTo(Arrays.asList(KeyValue.pair(userId + "@" +start+"->"+start, null),
                                             KeyValue.pair(userId + "@" +secondSessionStart
                                                           +"->"+secondSessionStart, null),
                                             KeyValue.pair(userId + "@"
                                                         +start+"->"+secondSessionStart,
                                                    3L))));

    // should only have the merged session in the store
    final List<KeyValue<Windowed<String>, Long>> mergedResults = fetchSessionsFromLocalStore(userId, playEventsPerSession);
    assertThat(mergedResults, equalTo(Collections.singletonList(KeyValue.pair(new Windowed<>(userId, new SessionWindow(start, secondSessionStart)), 3L))));




  }

  private List<KeyValue<Windowed<String>, Long>> fetchSessionsFromLocalStore(final String userId,
                                                                             final ReadOnlySessionStore<String, Long> playEventsPerSession) {
    final List<KeyValue<Windowed<String>, Long>> results = new ArrayList<>();
    try (final KeyValueIterator<Windowed<String>, Long> iterator = playEventsPerSession.fetch(userId)) {
      iterator.forEachRemaining(results::add);
    }
    return results;
  }

}
