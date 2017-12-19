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
package io.confluent.examples.streams.interactivequeries;

import com.google.common.collect.Sets;
import io.confluent.examples.streams.IntegrationTestUtils;
import io.confluent.examples.streams.kafka.EmbeddedSingleNodeKafkaCluster;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.ws.rs.NotFoundException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsCollectionContaining.hasItem;
import static org.hamcrest.core.IsEqual.equalTo;

/**
 * End-to-end integration test for {@link WordCountInteractiveQueriesExample}. Demonstrates
 * how you can programmatically query the REST API exposed by {@link WordCountInteractiveQueriesRestService}
 */
public class WordCountInteractiveQueriesExampleTest {

  @ClassRule
  public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();
  public static final String WORD_COUNT =
      "interactive-queries-wordcount-example-word-count-repartition";
  public static final String WINDOWED_WORD_COUNT =
      "interactive-queries-wordcount-example-windowed-word-count-repartition";

  @Rule
  public final TemporaryFolder temp = new TemporaryFolder();
  private KafkaStreams kafkaStreams;
  private WordCountInteractiveQueriesRestService proxy;

  @BeforeClass
  public static void createTopic() {
    CLUSTER.createTopic(WordCountInteractiveQueriesExample.TEXT_LINES_TOPIC, 2, 1);
    // The next two topics don't need to be created as they would be auto-created
    // by Kafka Streams, but it just makes the test more reliable if they already exist
    // as creating the topics causes a rebalance which closes the stores etc. So it makes
    // the timing quite difficult...
    CLUSTER.createTopic(WORD_COUNT, 2, 1);
    CLUSTER.createTopic(WINDOWED_WORD_COUNT, 2, 1);
  }

  @After
  public void shutdown() throws Exception {
    if (kafkaStreams != null) {
      kafkaStreams.close();
    }

    if (proxy != null) {
      proxy.stop();
    }

  }

  public static int randomFreeLocalPort() throws IOException {
    ServerSocket s = new ServerSocket(0);
    int port = s.getLocalPort();
    s.close();
    return port;
  }

  @Test
  public void shouldDemonstrateInteractiveQueries() throws Exception {
    final List<String> inputValues = Arrays.asList("hello",
        "world",
        "world",
        "hello world",
        "all streams lead to kafka",
        "streams",
        "kafka streams");

    Properties producerConfig = new Properties();
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
    producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
    producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    IntegrationTestUtils
        .produceValuesSynchronously(WordCountInteractiveQueriesExample.TEXT_LINES_TOPIC, inputValues,
                                    producerConfig);

    // Race condition caveat:  This two-step approach of finding a free port but not immediately
    // binding to it may cause occasional errors.
    final int port = randomFreeLocalPort();
    final String baseUrl = "http://localhost:" + port + "/state";

    kafkaStreams = WordCountInteractiveQueriesExample.createStreams(
        createStreamConfig(CLUSTER.bootstrapServers(), port, "one"));

    final CountDownLatch startupLatch = new CountDownLatch(1);
    kafkaStreams.setStateListener((newState, oldState) -> {
        if (newState == KafkaStreams.State.RUNNING && oldState == KafkaStreams.State.REBALANCING) {
          startupLatch.countDown();
        }
    });
    kafkaStreams.start();
    proxy = WordCountInteractiveQueriesExample.startRestProxy(kafkaStreams, port);

    assertTrue("streams failed to start within timeout", startupLatch.await(30, TimeUnit.SECONDS));

    final Client client = ClientBuilder.newClient();

    // Create a request to fetch all instances of HostStoreInfo
    final Invocation.Builder allInstancesRequest = client.target(baseUrl + "/instances")
        .request(MediaType.APPLICATION_JSON_TYPE);
    final List<HostStoreInfo> hostStoreInfo = fetchHostInfo(allInstancesRequest);

    assertThat(hostStoreInfo, hasItem(
        new HostStoreInfo("localhost", port, Sets.newHashSet("word-count", "windowed-word-count"))
    ));

    // Create a request to fetch all instances with word-count
    final Invocation.Builder wordCountInstancesRequest = client.target(baseUrl + "/instances/word-count")
        .request(MediaType.APPLICATION_JSON_TYPE);
    final List<HostStoreInfo>
        wordCountInstances = fetchHostInfo(wordCountInstancesRequest);

    assertThat(wordCountInstances, hasItem(
        new HostStoreInfo("localhost", port, Sets.newHashSet("word-count", "windowed-word-count"))
    ));

    // Fetch all key-value pairs from the word-count store
    final Invocation.Builder
        allRequest =
        client.target(baseUrl + "/keyvalues/word-count/all")
            .request(MediaType.APPLICATION_JSON_TYPE);

    final List<KeyValueBean>
        allValues =
        Arrays.asList(new KeyValueBean("all", 1L),
            new KeyValueBean("hello", 2L),
            new KeyValueBean("kafka", 2L),
            new KeyValueBean("lead", 1L),
            new KeyValueBean("streams", 3L),
            new KeyValueBean("to", 1L),
            new KeyValueBean("world", 3L));
    final List<KeyValueBean>
        all = fetchRangeOfValues(allRequest,
        allValues);
    assertThat(all, equalTo(allValues));

    // Fetch a range of key-value pairs from the word-count store
    final List<KeyValueBean> expectedRange = Arrays.asList(
        new KeyValueBean("all", 1L),
        new KeyValueBean("hello", 2L),
        new KeyValueBean("kafka", 2L));

    final Invocation.Builder
        request =
        client.target(baseUrl + "/keyvalues/word-count/range/all/kafka")
            .request(MediaType.APPLICATION_JSON_TYPE);
    final List<KeyValueBean>
        range = fetchRangeOfValues(request, expectedRange);

    assertThat(range, equalTo(expectedRange));

    // Find the instance of the Kafka Streams application that would have the key hello
    final HostStoreInfo
        hostWithHelloKey =
        client.target(baseUrl + "/instance/word-count/hello")
            .request(MediaType.APPLICATION_JSON_TYPE).get(HostStoreInfo.class);

    // Fetch the value for the key hello from the instance.
    final KeyValueBean result = client.target("http://" + hostWithHelloKey.getHost() +
        ":" + hostWithHelloKey.getPort() +
        "/state/keyvalue/word-count/hello")
        .request(MediaType.APPLICATION_JSON_TYPE).get(KeyValueBean.class);

    assertThat(result, equalTo(new KeyValueBean("hello", 2L)));

    // fetch windowed values for a key
    final List<KeyValueBean>
        windowedResult =
        client.target(baseUrl + "/windowed/windowed-word-count/streams/0/" + System
            .currentTimeMillis())
            .request(MediaType.APPLICATION_JSON_TYPE)

            .get(new GenericType<List<KeyValueBean>>() {
            });
    assertThat(windowedResult.size(), equalTo(1));
    final KeyValueBean keyValueBean = windowedResult.get(0);
    assertTrue(keyValueBean.getKey().startsWith("streams"));
    assertThat(keyValueBean.getValue(), equalTo(3L));
  }

  /**
   * We fetch these in a loop as they are the first couple of requests
   * directly after KafkaStreams.start(), so it can take some time
   * for the group to stabilize and all stores/instances to be available
   */
  private List<HostStoreInfo> fetchHostInfo(Invocation.Builder request)
      throws InterruptedException {
    List<HostStoreInfo> hostStoreInfo = request.get(new GenericType<List<HostStoreInfo>>() {
    });
    final long until = System.currentTimeMillis() + 60000L;
    while (hostStoreInfo.isEmpty()
        || hostStoreInfo.get(0).getStoreNames().size() != 2
        && System.currentTimeMillis() < until) {
      Thread.sleep(10);
      hostStoreInfo = request.get(new GenericType<List<HostStoreInfo>>() {
      });
    }
    return hostStoreInfo;
  }

  private List<KeyValueBean> fetchRangeOfValues(final Invocation.Builder request,
                                                final List<KeyValueBean>
                                                    expectedResults) {
    List<KeyValueBean> results = new ArrayList<>();
    final long timeout = System.currentTimeMillis() + 10000L;
    while (!results.containsAll(expectedResults) && System.currentTimeMillis() < timeout) {
      try {
        results = request.get(new GenericType<List<KeyValueBean>>() {
        });
      } catch (NotFoundException e) {
        //
      }
    }
    Collections.sort(results, (o1, o2) -> o1.getKey().compareTo(o2.getKey()));
    return results;
  }

  private Properties createStreamConfig(final String bootStrap, final int port, String stateDir)
      throws
      IOException {
    Properties streamsConfiguration = new Properties();
    // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
    // against which the application is run.
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG,
        "interactive-queries-wordcount-example");
    // Where to find Kafka broker(s).
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrap);
    // The host:port the embedded REST proxy will run on
    streamsConfiguration.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:" + port);
    // The directory where the RocksDB State Stores will reside
    streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, temp.newFolder(stateDir).getPath());
    // Set the default key serde
    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    // Set the default value serde
    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    return streamsConfiguration;
  }
}