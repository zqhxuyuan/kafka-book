package io.confluent.examples.streams.microservices;

import static io.confluent.examples.streams.avro.microservices.OrderState.CREATED;
import static io.confluent.examples.streams.avro.microservices.Product.JUMPERS;
import static io.confluent.examples.streams.avro.microservices.Product.UNDERPANTS;
import static io.confluent.examples.streams.microservices.domain.Schemas.Topics;
import static io.confluent.examples.streams.microservices.domain.beans.OrderId.id;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

import io.confluent.examples.streams.IntegrationTestUtils;
import io.confluent.examples.streams.avro.microservices.Order;
import io.confluent.examples.streams.avro.microservices.OrderValidation;
import io.confluent.examples.streams.avro.microservices.OrderValidationResult;
import io.confluent.examples.streams.avro.microservices.OrderValidationType;
import io.confluent.examples.streams.avro.microservices.Product;
import io.confluent.examples.streams.kafka.EmbeddedSingleNodeKafkaCluster;
import io.confluent.examples.streams.microservices.domain.Schemas;
import io.confluent.examples.streams.microservices.util.MicroserviceTestUtils;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

public class InventoryServiceTest extends MicroserviceTestUtils {

  private List<KeyValue<Product, Integer>> inventory;
  private List<Order> orders;
  private List<OrderValidation> expected;
  private InventoryService inventoryService;


  @BeforeClass
  public static void startKafkaCluster() throws Exception {
    CLUSTER.createTopic(Topics.ORDERS.name());
    CLUSTER.createTopic(Topics.ORDER_VALIDATIONS.name());
    Schemas.configureSerdesWithSchemaRegistryUrl(CLUSTER.schemaRegistryUrl());
  }

  @Test
  public void shouldProcessOrdersWithSufficientStockAndRejectOrdersWithInsufficientStock()
      throws Exception {

    //Given
    inventoryService = new InventoryService();

    inventory = asList(
        new KeyValue<>(UNDERPANTS, 75),
        new KeyValue<>(JUMPERS, 1)
    );
    sendInventory(inventory, Topics.WAREHOUSE_INVENTORY);

    orders = asList(
        new Order(id(0L), 1L, CREATED, UNDERPANTS, 3, 10.00d),
        new Order(id(1L), 2L, CREATED, JUMPERS, 1, 75.00d),
        new Order(id(2L), 2L, CREATED, JUMPERS, 1, 75.00d)
    );
    sendOrders(orders);

    //When
    inventoryService.start(CLUSTER.bootstrapServers());

    //Then the final order for Jumpers should have been 'rejected' as it's out of stock
    expected = asList(
        new OrderValidation(id(0L), OrderValidationType.INVENTORY_CHECK,
            OrderValidationResult.PASS),
        new OrderValidation(id(1L), OrderValidationType.INVENTORY_CHECK,
            OrderValidationResult.PASS),
        new OrderValidation(id(2L), OrderValidationType.INVENTORY_CHECK, OrderValidationResult.FAIL)
    );
    assertThat(MicroserviceTestUtils
        .read(Topics.ORDER_VALIDATIONS, expected.size(), CLUSTER.bootstrapServers()))
        .isEqualTo(expected);

    //And the reservations should have been incremented twice, once for each validated order
    List<KeyValue<Product, Long>> inventoryChangelog = readInventoryStateStore(2);
    assertThat(inventoryChangelog).isEqualTo(asList(
        new KeyValue<>(UNDERPANTS.toString(), 3L),
        new KeyValue<>(JUMPERS.toString(), 1L)
    ));
  }

  private List<KeyValue<Product, Long>> readInventoryStateStore(int numberOfRecordsToWaitFor)
      throws InterruptedException {
    return IntegrationTestUtils
        .waitUntilMinKeyValueRecordsReceived(inventoryConsumerProperties(CLUSTER),
            ProcessorStateManager.storeChangelogTopic(InventoryService.INVENTORY_SERVICE_APP_ID,
                InventoryService.RESERVED_STOCK_STORE_NAME), numberOfRecordsToWaitFor);
  }

  private static Properties inventoryConsumerProperties(EmbeddedSingleNodeKafkaCluster cluster) {
    Properties consumerConfig = new Properties();
    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "inventory-test-reader");
    consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
    return consumerConfig;
  }

  @After
  public void tearDown() {
    inventoryService.stop();
  }
}