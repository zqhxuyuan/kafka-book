package io.confluent.examples.streams.microservices;

import static io.confluent.examples.streams.avro.microservices.OrderState.CREATED;
import static io.confluent.examples.streams.avro.microservices.OrderValidationResult.FAIL;
import static io.confluent.examples.streams.avro.microservices.OrderValidationResult.PASS;
import static io.confluent.examples.streams.avro.microservices.OrderValidationType.FRAUD_CHECK;
import static io.confluent.examples.streams.avro.microservices.Product.JUMPERS;
import static io.confluent.examples.streams.avro.microservices.Product.UNDERPANTS;
import static io.confluent.examples.streams.microservices.domain.Schemas.Topics;
import static io.confluent.examples.streams.microservices.domain.beans.OrderId.id;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

import io.confluent.examples.streams.avro.microservices.Order;
import io.confluent.examples.streams.avro.microservices.OrderValidation;
import io.confluent.examples.streams.microservices.domain.Schemas;
import io.confluent.examples.streams.microservices.util.MicroserviceTestUtils;
import java.util.List;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

public class FraudServiceTest extends MicroserviceTestUtils {

  private List<Order> orders;
  private List<OrderValidation> expected;
  private FraudService fraudService;

  @BeforeClass
  public static void startKafkaCluster() throws Exception {
    if (!CLUSTER.isRunning()) {
      CLUSTER.start();
    }

    CLUSTER.createTopic(Topics.ORDERS.name());
    CLUSTER.createTopic(Topics.ORDER_VALIDATIONS.name());
    Schemas.configureSerdesWithSchemaRegistryUrl(CLUSTER.schemaRegistryUrl());
  }

  @After
  public void tearDown() throws Exception {
    fraudService.stop();
    CLUSTER.stop();
  }

  @Test
  public void shouldValidateWhetherOrderAmountExceedsFraudLimitOverWindow() throws Exception {
    //Given
    fraudService = new FraudService();

    orders = asList(
        new Order(id(0L), 0L, CREATED, UNDERPANTS, 3, 5.00d),
        new Order(id(1L), 0L, CREATED, JUMPERS, 1, 75.00d),
        new Order(id(2L), 1L, CREATED, JUMPERS, 1, 75.00d),
        new Order(id(3L), 1L, CREATED, JUMPERS, 1, 75.00d),
        new Order(id(4L), 1L, CREATED, JUMPERS, 50, 75.00d),    //Should fail as over limit
        new Order(id(5L), 2L, CREATED, UNDERPANTS, 10, 100.00d),//First should pass
        new Order(id(6L), 2L, CREATED, UNDERPANTS, 10, 100.00d),//Second should fail as rolling total by customer is over limit
        new Order(id(7L), 2L, CREATED, UNDERPANTS, 1, 5.00d)    //Third should fail as rolling total by customer is still over limit
    );
    sendOrders(orders);

    //When
    fraudService.start(CLUSTER.bootstrapServers());

    //Then there should be failures for the two orders that push customers over their limit.
    expected = asList(
        new OrderValidation(id(0L), FRAUD_CHECK, PASS),
        new OrderValidation(id(1L), FRAUD_CHECK, PASS),
        new OrderValidation(id(2L), FRAUD_CHECK, PASS),
        new OrderValidation(id(3L), FRAUD_CHECK, PASS),
        new OrderValidation(id(4L), FRAUD_CHECK, FAIL),
        new OrderValidation(id(5L), FRAUD_CHECK, PASS),
        new OrderValidation(id(6L), FRAUD_CHECK, FAIL),
        new OrderValidation(id(7L), FRAUD_CHECK, FAIL)
    );
    List<OrderValidation> read = read(Topics.ORDER_VALIDATIONS, 8, CLUSTER.bootstrapServers());
    assertThat(read).isEqualTo(expected);
  }
}