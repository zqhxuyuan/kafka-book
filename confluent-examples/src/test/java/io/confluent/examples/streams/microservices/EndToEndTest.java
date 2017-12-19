package io.confluent.examples.streams.microservices;

import static io.confluent.examples.streams.avro.microservices.Product.JUMPERS;
import static io.confluent.examples.streams.avro.microservices.Product.UNDERPANTS;
import static io.confluent.examples.streams.microservices.domain.beans.OrderId.id;
import static io.confluent.examples.streams.microservices.util.MicroserviceUtils.MIN;
import static io.confluent.examples.streams.microservices.util.MicroserviceUtils.randomFreeLocalPort;
import static java.util.Arrays.asList;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import io.confluent.examples.streams.avro.microservices.OrderState;
import io.confluent.examples.streams.avro.microservices.Product;
import io.confluent.examples.streams.microservices.domain.Schemas;
import io.confluent.examples.streams.microservices.domain.Schemas.Topics;
import io.confluent.examples.streams.microservices.domain.beans.OrderBean;
import io.confluent.examples.streams.microservices.util.MicroserviceTestUtils;
import io.confluent.examples.streams.microservices.util.Paths;
import java.util.ArrayList;
import java.util.List;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.GenericType;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.HostInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EndToEndTest extends MicroserviceTestUtils {

  private static final Logger log = LoggerFactory.getLogger(EndToEndTest.class);
  private static final String HOST = "localhost";
  private List<Service> services = new ArrayList<>();
  private static int restPort;
  private OrderBean returnedBean;
  private long startTime;
  private Paths path;

  @Test
  public void shouldCreateNewOrderAndGetBackValidatedOrder() throws Exception {
    OrderBean inputOrder = new OrderBean(id(1L), 2L, OrderState.CREATED, Product.JUMPERS, 1, 1d);
    Client client = ClientBuilder.newClient();

    //Add inventory required by the inventory service with enough items in stock to pass validation
    List<KeyValue<Product, Integer>> inventory = asList(
        new KeyValue<>(UNDERPANTS, 75),
        new KeyValue<>(JUMPERS, 10)
    );
    sendInventory(inventory, Topics.WAREHOUSE_INVENTORY);

    //When we POST order and immediately GET on the returned location
    client.target(path.urlPost()).request(APPLICATION_JSON_TYPE).post(Entity.json(inputOrder));
    returnedBean = client.target(path.urlGetValidated(1)).queryParam("timeout", MIN / 2)
        .request(APPLICATION_JSON_TYPE).get(newBean());

    //Then
    assertThat(returnedBean.getState()).isEqualTo(OrderState.VALIDATED);
  }

  @Test
  public void shouldProcessManyValidOrdersEndToEnd() throws Exception {
    Client client = ClientBuilder.newClient();

    //Add inventory required by the inventory service
    List<KeyValue<Product, Integer>> inventory = asList(
        new KeyValue<>(UNDERPANTS, 75),
        new KeyValue<>(JUMPERS, 10)
    );
    sendInventory(inventory, Topics.WAREHOUSE_INVENTORY);

    //Send ten orders in succession
    for (int i = 0; i < 10; i++) {
      OrderBean inputOrder = new OrderBean(id(i), 2L, OrderState.CREATED, Product.JUMPERS, 1, 1d);

      startTimer();

      //POST & GET order
      client.target(path.urlPost()).request(APPLICATION_JSON_TYPE).post(Entity.json(inputOrder));
      returnedBean = client.target(path.urlGetValidated(i)).queryParam("timeout", MIN / 2)
          .request(APPLICATION_JSON_TYPE).get(newBean());

      endTimer();

      assertThat(returnedBean).isEqualTo(new OrderBean(
          inputOrder.getId(),
          inputOrder.getCustomerId(),
          OrderState.VALIDATED,
          inputOrder.getProduct(),
          inputOrder.getQuantity(),
          inputOrder.getPrice()
      ));
    }
  }

  @Test
  public void shouldProcessManyInvalidOrdersEndToEnd() throws Exception {
    final Client client = ClientBuilder.newClient();

    //Add inventory required by the inventory service
    List<KeyValue<Product, Integer>> inventory = asList(
        new KeyValue<>(UNDERPANTS, 75000),
        new KeyValue<>(JUMPERS, 0) //***nothing in stock***
    );
    sendInventory(inventory, Topics.WAREHOUSE_INVENTORY);

    //Send ten orders one after the other
    for (int i = 0; i < 10; i++) {
      OrderBean inputOrder = new OrderBean(id(i), 2L, OrderState.CREATED, Product.JUMPERS, 1, 1d);

      startTimer();

      //POST & GET order
      client.target(path.urlPost()).request(APPLICATION_JSON_TYPE).post(Entity.json(inputOrder));
      returnedBean = client.target(path.urlGetValidated(i)).queryParam("timeout", MIN / 2)
          .request(APPLICATION_JSON_TYPE).get(newBean());

      endTimer();

      assertThat(returnedBean).isEqualTo(new OrderBean(
          inputOrder.getId(),
          inputOrder.getCustomerId(),
          OrderState.FAILED,
          inputOrder.getProduct(),
          inputOrder.getQuantity(),
          inputOrder.getPrice()
      ));
    }
  }

  private void startTimer() {
    startTime = System.currentTimeMillis();
  }

  private void endTimer() {
    log.info("Took: " + (System.currentTimeMillis() - startTime));
  }

  @Before
  public void startEverythingElse() throws Exception {
    if (!CLUSTER.isRunning()) {
      CLUSTER.start();
    }

    Topics.ALL.keySet().forEach(CLUSTER::createTopic);
    Schemas.configureSerdesWithSchemaRegistryUrl(CLUSTER.schemaRegistryUrl());
    restPort = randomFreeLocalPort();

    services.add(new FraudService());
    services.add(new InventoryService());
    services.add(new OrderDetailsService());
    services.add(new ValidationsAggregatorService());
    services.add(new OrdersService(new HostInfo(HOST, restPort)));

    tailAllTopicsToConsole(CLUSTER.bootstrapServers());
    services.forEach(s -> s.start(CLUSTER.bootstrapServers()));

    path = new Paths("localhost", restPort);
  }

  @After
  public void tearDown() throws Exception {
    services.forEach(Service::stop);
    stopTailers();
    CLUSTER.stop();
  }

  private GenericType<OrderBean> newBean() {
    return new GenericType<OrderBean>() {
    };
  }
}
