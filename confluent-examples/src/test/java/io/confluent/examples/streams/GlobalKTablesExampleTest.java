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
 */package io.confluent.examples.streams;

import io.confluent.examples.streams.avro.Customer;
import io.confluent.examples.streams.avro.EnrichedOrder;
import io.confluent.examples.streams.avro.Order;
import io.confluent.examples.streams.avro.Product;
import io.confluent.examples.streams.kafka.EmbeddedSingleNodeKafkaCluster;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;
import java.util.Properties;

import static io.confluent.examples.streams.GlobalKTablesExample.CUSTOMER_STORE;
import static io.confluent.examples.streams.GlobalKTablesExample.CUSTOMER_TOPIC;
import static io.confluent.examples.streams.GlobalKTablesExample.ENRICHED_ORDER_TOPIC;
import static io.confluent.examples.streams.GlobalKTablesExample.ORDER_TOPIC;
import static io.confluent.examples.streams.GlobalKTablesExample.PRODUCT_STORE;
import static io.confluent.examples.streams.GlobalKTablesExample.PRODUCT_TOPIC;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsCollectionContaining.hasItem;

public class GlobalKTablesExampleTest {

  @ClassRule
  public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();
  private KafkaStreams streamInstanceOne;
  private KafkaStreams streamInstanceTwo;

  @BeforeClass
  public static void createTopics() {
    CLUSTER.createTopic(ORDER_TOPIC, 4, 1);
    CLUSTER.createTopic(CUSTOMER_TOPIC, 3, 1);
    CLUSTER.createTopic(PRODUCT_TOPIC, 2, 1);
    CLUSTER.createTopic(ENRICHED_ORDER_TOPIC);
  }

  @Before
  public void createStreams() throws Exception {
    // start two instances of streams to demonstrate that GlobalKTables are
    // replicated on each instance
    streamInstanceOne =
        GlobalKTablesExample.createStreams(CLUSTER.bootstrapServers(),
                                           CLUSTER.schemaRegistryUrl(),
                                           TestUtils.tempDirectory().getPath());

    streamInstanceTwo =
        GlobalKTablesExample.createStreams(CLUSTER.bootstrapServers(),
                                           CLUSTER.schemaRegistryUrl(),
                                           TestUtils.tempDirectory().getPath());
  }

  @After
  public void close() {
    streamInstanceOne.close();
    streamInstanceTwo.close();
  }

  @Test
  public void shouldDemonstrateGlobalKTableJoins() throws Exception {
    final List<Customer> customers = GlobalKTablesExampleDriver.generateCustomers(CLUSTER.bootstrapServers(),
                                                                                  CLUSTER.schemaRegistryUrl(),
                                                                                  100);

    final List<Product> products = GlobalKTablesExampleDriver.generateProducts(CLUSTER.bootstrapServers(),
                                                                               CLUSTER.schemaRegistryUrl(),
                                                                               100);

    final List<Order> orders = GlobalKTablesExampleDriver.generateOrders(CLUSTER.bootstrapServers(),
                                                                         CLUSTER.schemaRegistryUrl(),
                                                                         100,
                                                                         100,
                                                                         50);
    // start up the streams instances
    streamInstanceOne.start();
    streamInstanceTwo.start();

    final Properties consumerProps = new Properties();
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "global-tables-consumer");
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Serdes.Long().deserializer().getClass());
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
    consumerProps.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, CLUSTER.schemaRegistryUrl());
    consumerProps.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

    // receive the enriched orders
    final List<EnrichedOrder>
        enrichedOrders =
        IntegrationTestUtils.waitUntilMinValuesRecordsReceived(consumerProps, ENRICHED_ORDER_TOPIC, 50, 60000);

    // verify that all the data comes from the generated set
    for (final EnrichedOrder enrichedOrder : enrichedOrders) {
      assertThat(customers, hasItem(enrichedOrder.getCustomer()));
      assertThat(products, hasItem(enrichedOrder.getProduct()));
      assertThat(orders, hasItem(enrichedOrder.getOrder()));
    }

    // demonstrate that global table data is available on all instances
    verifyAllCustomersInStore(customers, streamInstanceOne.store(CUSTOMER_STORE, QueryableStoreTypes.keyValueStore()));
    verifyAllCustomersInStore(customers, streamInstanceTwo.store(CUSTOMER_STORE, QueryableStoreTypes.keyValueStore()));
    verifyAllProductsInStore(products, streamInstanceOne.store(PRODUCT_STORE, QueryableStoreTypes.keyValueStore()));
    verifyAllProductsInStore(products, streamInstanceTwo.store(PRODUCT_STORE, QueryableStoreTypes.keyValueStore()));
  }

  private void verifyAllCustomersInStore(final List<Customer> customers,
                                        final ReadOnlyKeyValueStore<Long, Customer> store) {
    for (long id = 0; id < customers.size(); id++) {
      assertThat(store.get(id), equalTo(customers.get((int)id)));
    }
  }

  private void verifyAllProductsInStore(final List<Product> products,
                                         final ReadOnlyKeyValueStore<Long, Product> store) {
    for (long id = 0; id < products.size(); id++) {
      assertThat(store.get(id), equalTo(products.get((int)id)));
    }
  }

}
