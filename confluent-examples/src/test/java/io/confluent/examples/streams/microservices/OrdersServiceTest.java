package io.confluent.examples.streams.microservices;

import static io.confluent.examples.streams.avro.microservices.Order.newBuilder;
import static io.confluent.examples.streams.microservices.domain.beans.OrderId.id;
import static io.confluent.examples.streams.microservices.util.MicroserviceUtils.MIN;
import static io.confluent.examples.streams.microservices.util.MicroserviceUtils.randomFreeLocalPort;
import static java.util.Arrays.asList;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.junit.Assert.fail;

import io.confluent.examples.streams.avro.microservices.Order;
import io.confluent.examples.streams.avro.microservices.OrderState;
import io.confluent.examples.streams.avro.microservices.Product;
import io.confluent.examples.streams.microservices.domain.Schemas;
import io.confluent.examples.streams.microservices.domain.Schemas.Topics;
import io.confluent.examples.streams.microservices.domain.beans.OrderBean;
import io.confluent.examples.streams.microservices.util.MicroserviceTestUtils;
import io.confluent.examples.streams.microservices.util.Paths;
import java.net.HttpURLConnection;
import javax.ws.rs.ServerErrorException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import org.apache.kafka.streams.state.HostInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class OrdersServiceTest extends MicroserviceTestUtils {

  private int port;
  private OrdersService rest;
  private OrdersService rest2;
  private Paths paths;

  @BeforeClass
  public static void startKafkaCluster() throws Exception {
    CLUSTER.createTopic(Topics.ORDERS.name());
    Schemas.configureSerdesWithSchemaRegistryUrl(CLUSTER.schemaRegistryUrl());
  }

  @Before
  public void start() throws Exception {
    port = randomFreeLocalPort();
    paths = new Paths("localhost", port);
  }

  @After
  public void shutdown() throws Exception {
    if (rest != null) {
      rest.stop();
    }
    if (rest2 != null) {
      rest2.stop();
    }
  }

  @Test
  public void shouldPostOrderAndGetItBack() throws Exception {
    OrderBean bean = new OrderBean(id(1L), 2L, OrderState.CREATED, Product.JUMPERS, 10, 100d);

    final Client client = ClientBuilder.newClient();

    //Given a rest service
    rest = new OrdersService(new HostInfo("localhost", port));
    rest.start(CLUSTER.bootstrapServers());

    //When we POST an order
    Response response = client.target(paths.urlPost())
        .request(APPLICATION_JSON_TYPE)
        .post(Entity.json(bean));

    //Then
    assertThat(response.getStatus()).isEqualTo(HttpURLConnection.HTTP_CREATED);

    //When GET the bean back via it's location
    OrderBean returnedBean = client.target(response.getLocation())
        .queryParam("timeout", MIN / 2)
        .request(APPLICATION_JSON_TYPE)
        .get(new GenericType<OrderBean>() {
        });

    //Then it should be the bean we PUT
    assertThat(returnedBean).isEqualTo(bean);

    //When GET the bean back explicitly
    returnedBean = client.target(paths.urlGet(1))
        .queryParam("timeout", MIN / 2)
        .request(APPLICATION_JSON_TYPE)
        .get(new GenericType<OrderBean>() {
        });

    //Then it should be the bean we PUT
    assertThat(returnedBean).isEqualTo(bean);
  }


  @Test
  public void shouldGetValidatedOrderOnRequest() throws Exception {
    Order orderV1 = new Order(id(1L), 2L, OrderState.CREATED, Product.JUMPERS, 10, 100d);
    OrderBean beanV1 = OrderBean.toBean(orderV1);

    final Client client = ClientBuilder.newClient();

    //Given a rest service
    rest = new OrdersService(
        new HostInfo("localhost", port)
    );
    rest.start(CLUSTER.bootstrapServers());

    //When we post an order
    client.target(paths.urlPost())
        .request(APPLICATION_JSON_TYPE)
        .post(Entity.json(beanV1));

    //Simulate the order being validated
    MicroserviceTestUtils.sendOrders(asList(
        newBuilder(orderV1)
            .setState(OrderState.VALIDATED)
            .build()));

    //When we GET the order from the returned location
    OrderBean returnedBean = client.target(paths.urlGetValidated(beanV1.getId()))
        .queryParam("timeout", MIN / 2)
        .request(APPLICATION_JSON_TYPE)
        .get(new GenericType<OrderBean>() {
        });

    //Then status should be Validated
    assertThat(returnedBean.getState()).isEqualTo(OrderState.VALIDATED);
  }

  @Test
  public void shouldTimeoutGetIfNoResponseIsFound() throws Exception {
    final Client client = ClientBuilder.newClient();

    //Start the rest interface
    rest = new OrdersService(
        new HostInfo("localhost", port)
    );
    rest.start(CLUSTER.bootstrapServers());

    //Then GET order should timeout
    try {
      client.target(paths.urlGet(1))
          .queryParam("timeout", 100) //Lower the request timeout
          .request(APPLICATION_JSON_TYPE)
          .get(new GenericType<OrderBean>() {
          });
      fail("Request should have failed as materialized view has not been updated");
    } catch (ServerErrorException e) {
      assertThat(e.getMessage()).isEqualTo("HTTP 504 Gateway Timeout");
    }
  }

  @Test
  public void shouldGetOrderByIdWhenOnDifferentHost() throws Exception {
    OrderBean order = new OrderBean(id(1L), 2L, OrderState.VALIDATED, Product.JUMPERS, 10, 100d);
    int port1 = randomFreeLocalPort();
    int port2 = randomFreeLocalPort();
    final Client client = ClientBuilder.newClient();

    //Given two rest servers on different ports
    Paths paths1 = new Paths("localhost", port1);
    Paths paths2 = new Paths("localhost", port2);
    rest = new OrdersService(new HostInfo("localhost", port1));
    rest.start(CLUSTER.bootstrapServers());
    rest2 = new OrdersService(new HostInfo("localhost", port2));
    rest2.start(CLUSTER.bootstrapServers());

    //And one order
    client.target(paths1.urlPost())
        .request(APPLICATION_JSON_TYPE)
        .post(Entity.json(order));

    //When GET to rest1
    OrderBean returnedOrder = client.target(paths1.urlGet(order.getId()))
        .queryParam("timeout", MIN / 2)
        .request(APPLICATION_JSON_TYPE)
        .get(new GenericType<OrderBean>() {
        });

    //Then we should get the order back
    assertThat(returnedOrder).isEqualTo(order);

    //When GET to rest2
    returnedOrder = client.target(paths2.urlGet(order.getId()))
        .queryParam("timeout", MIN / 2)
        .request(APPLICATION_JSON_TYPE)
        .get(new GenericType<OrderBean>() {
        });

    //Then we should get the order back also
    assertThat(returnedOrder).isEqualTo(order);
  }
}
