package io.confluent.examples.streams.microservices;

import static io.confluent.examples.streams.avro.microservices.Order.newBuilder;
import static io.confluent.examples.streams.avro.microservices.OrderState.VALIDATED;
import static io.confluent.examples.streams.avro.microservices.OrderValidationResult.FAIL;
import static io.confluent.examples.streams.avro.microservices.OrderValidationResult.PASS;
import static io.confluent.examples.streams.microservices.domain.Schemas.Topics.ORDERS;
import static io.confluent.examples.streams.microservices.domain.Schemas.Topics.ORDER_VALIDATIONS;
import static io.confluent.examples.streams.microservices.util.MicroserviceUtils.MIN;
import static io.confluent.examples.streams.microservices.util.MicroserviceUtils.addShutdownHookAndBlock;
import static io.confluent.examples.streams.microservices.util.MicroserviceUtils.baseStreamsConfig;
import static io.confluent.examples.streams.microservices.util.MicroserviceUtils.parseArgsAndConfigure;

import io.confluent.examples.streams.avro.microservices.Order;
import io.confluent.examples.streams.avro.microservices.OrderState;
import io.confluent.examples.streams.avro.microservices.OrderValidation;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A simple service which listens to to validation results from each of the Validation
 * services and aggregates them by order Id, triggering a pass or fail based on whether
 * all rules pass or not.
 */
public class ValidationsAggregatorService implements Service {

  private static final Logger log = LoggerFactory.getLogger(ValidationsAggregatorService.class);
  private static final String ORDERS_SERVICE_APP_ID = "orders-service";
  private KafkaStreams streams;

  @Override
  public void start(String bootstrapServers) {
    streams = aggregateOrderValidations(bootstrapServers, "/tmp/kafka-streams");
    streams.cleanUp(); //don't do this in prod as it clears your state stores
    streams.start();
    log.info("Started Service " + getClass().getSimpleName());
  }

  private KafkaStreams aggregateOrderValidations(String bootstrapServers, String stateDir) {
    final int numberOfRules = 3; //TODO put into a KTable to make dynamically configurable

    KStreamBuilder builder = new KStreamBuilder();
    KStream<String, OrderValidation> validations = builder
        .stream(ORDER_VALIDATIONS.keySerde(), ORDER_VALIDATIONS.valueSerde(), ORDER_VALIDATIONS.name());
    KStream<String, Order> orders = builder
        .stream(ORDERS.keySerde(), ORDERS.valueSerde(), ORDERS.name())
        .filter((id, order) -> OrderState.CREATED.equals(order.getState()));

    //If all rules pass then validate the order
    validations.groupByKey(ORDER_VALIDATIONS.keySerde(), ORDER_VALIDATIONS.valueSerde())
        .aggregate(
            () -> 0L,
            (id, result, total) -> PASS.equals(result.getValidationResult()) ? total + 1 : total,
            TimeWindows.of(5 * MIN),
            Serdes.Long()
        )
        //get rid of window
        .toStream((windowedKey, total) -> windowedKey.key())
        //only include results were all rules passed validation
        .filter((k, total) -> total >= numberOfRules)
        //Join back to orders
        .join(orders, (id, order) ->
                //Set the order to Validated and bump the version on it's ID
                newBuilder(order).setState(VALIDATED).build()
            , JoinWindows.of(5 * MIN), ORDERS.keySerde(), Serdes.Long(), ORDERS.valueSerde())
        //Push the validated order into the orders topic
        .to(ORDERS.keySerde(), ORDERS.valueSerde(), ORDERS.name());

    //If any rule fails then fail the order
    validations.filter((id, rule) -> FAIL.equals(rule.getValidationResult()))
        .join(orders, (id, order) ->
                //Set the order to Failed and bump the version on it's ID
                newBuilder(order).setState(OrderState.FAILED).build(),
            JoinWindows.of(5 * MIN), ORDERS.keySerde(), ORDER_VALIDATIONS.valueSerde(),
            ORDERS.valueSerde())
        //there could be multiple failed rules for each order so collapse to a single order
        .groupByKey(ORDERS.keySerde(), ORDERS.valueSerde())
        .reduce((order, v1) -> order)
        //Push the validated order into the orders topic
        .to(ORDERS.keySerde(), ORDERS.valueSerde(), ORDERS.name());

    return new KafkaStreams(builder,
        baseStreamsConfig(bootstrapServers, stateDir, ORDERS_SERVICE_APP_ID));
  }

  @Override
  public void stop() {
    if (streams != null) {
      streams.close();
    }
  }

  public static void main(String[] args) throws Exception {
    ValidationsAggregatorService service = new ValidationsAggregatorService();
    service.start(parseArgsAndConfigure(args));
    addShutdownHookAndBlock(service);
  }
}