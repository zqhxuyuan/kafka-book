package io.confluent.examples.streams.microservices;

import static io.confluent.examples.streams.avro.microservices.OrderState.CREATED;
import static io.confluent.examples.streams.avro.microservices.Product.JUMPERS;
import static io.confluent.examples.streams.avro.microservices.Product.UNDERPANTS;
import static io.confluent.examples.streams.microservices.domain.Schemas.Topics;
import static io.confluent.examples.streams.microservices.domain.beans.OrderId.id;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

import io.confluent.examples.streams.avro.microservices.Order;
import io.confluent.examples.streams.avro.microservices.OrderValidation;
import io.confluent.examples.streams.avro.microservices.OrderValidationResult;
import io.confluent.examples.streams.avro.microservices.OrderValidationType;
import io.confluent.examples.streams.microservices.domain.Schemas;
import io.confluent.examples.streams.microservices.util.MicroserviceTestUtils;
import java.util.List;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

public class OrderDetailsServiceTest extends MicroserviceTestUtils {

  private List<Order> orders;
  private List<OrderValidation> expected;
  private OrderDetailsService orderValService;


  @BeforeClass
  public static void startKafkaCluster() throws Exception {
    CLUSTER.createTopic(Topics.ORDERS.name());
    CLUSTER.createTopic(Topics.ORDER_VALIDATIONS.name());
    Schemas.configureSerdesWithSchemaRegistryUrl(CLUSTER.schemaRegistryUrl());
  }

  @Test
  public void shouldPassAndFailOrders() throws Exception {
    //Given
    orderValService = new OrderDetailsService();

    orders = asList(
        new Order(id(0L), 0L, CREATED, UNDERPANTS, 3, 5.00d), //should pass
        new Order(id(1L), 0L, CREATED, JUMPERS, -1, 75.00d) //should fail
    );
    sendOrders(orders);

    //When
    orderValService.start(CLUSTER.bootstrapServers());

    //Then the second order for Jumpers should have been 'rejected' as it's out of stock
    expected = asList(
        new OrderValidation(id(0L), OrderValidationType.ORDER_DETAILS_CHECK,
            OrderValidationResult.PASS),
        new OrderValidation(id(1L), OrderValidationType.ORDER_DETAILS_CHECK,
            OrderValidationResult.FAIL)
    );
    assertThat(MicroserviceTestUtils.read(Topics.ORDER_VALIDATIONS, 2, CLUSTER.bootstrapServers()))
        .isEqualTo(expected);
  }

  @After
  public void tearDown() {
    orderValService.stop();
  }
}