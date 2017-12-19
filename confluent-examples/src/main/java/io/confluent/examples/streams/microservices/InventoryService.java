package io.confluent.examples.streams.microservices;

import static io.confluent.examples.streams.avro.microservices.OrderValidationResult.FAIL;
import static io.confluent.examples.streams.avro.microservices.OrderValidationResult.PASS;
import static io.confluent.examples.streams.avro.microservices.OrderValidationType.INVENTORY_CHECK;
import static io.confluent.examples.streams.microservices.util.MicroserviceUtils.addShutdownHookAndBlock;
import static io.confluent.examples.streams.microservices.util.MicroserviceUtils.parseArgsAndConfigure;

import io.confluent.examples.streams.avro.microservices.Order;
import io.confluent.examples.streams.avro.microservices.OrderState;
import io.confluent.examples.streams.avro.microservices.OrderValidation;
import io.confluent.examples.streams.avro.microservices.Product;
import io.confluent.examples.streams.microservices.domain.Schemas.Topics;
import io.confluent.examples.streams.microservices.util.MicroserviceUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This service validates incoming orders to ensure there is sufficient stock to
 * fulfill them. This validation process considers both the inventory in the warehouse
 * as well as a set "reserved" items which is maintained by this service. Reserved
 * items are those that are in the warehouse, but have been allocated to a pending
 * order.
 * <p>
 * Currently there is nothing implemented that decrements the reserved items. This
 * would happen, inside this service, in response to an order being shipped.
 */
public class InventoryService implements Service {

  private static final Logger log = LoggerFactory.getLogger(InventoryService.class);
  public static final String INVENTORY_SERVICE_APP_ID = "inventory-service";
  public static final String RESERVED_STOCK_STORE_NAME = "store-of-reserved-stock";
  private KafkaStreams streams;

  @Override
  public void start(String bootstrapServers) {
    streams = processStreams(bootstrapServers, "/tmp/kafka-streams");
    streams.cleanUp(); //don't do this in prod as it clears your state stores
    streams.start();
    log.info("Started Service " + getClass().getSimpleName());
  }

  @Override
  public void stop() {
    if (streams != null) {
      streams.close();
    }
  }

  private KafkaStreams processStreams(final String bootstrapServers, final String stateDir) {

    //Latch onto instances of the orders and inventory topics
    KStreamBuilder builder = new KStreamBuilder();
    KStream<String, Order> orders = builder.stream(Topics.ORDERS.keySerde(), Topics.ORDERS.valueSerde(), Topics.ORDERS.name());
    KTable<Product, Integer> warehouseInventory = builder.table(Topics.WAREHOUSE_INVENTORY.keySerde(), Topics.WAREHOUSE_INVENTORY.valueSerde(), Topics.WAREHOUSE_INVENTORY.name());

    //Create a store to reserve inventory whilst the order is processed.
    //This will be prepopulated from Kafka before the service starts processing
    StateStoreSupplier reservedStock = Stores.create(RESERVED_STOCK_STORE_NAME)
        .withKeys(Topics.WAREHOUSE_INVENTORY.keySerde()).withValues(Serdes.Long())
        .persistent().build();
    builder.addStateStore(reservedStock);

    //First change orders stream to be keyed by Product (so we can join with warehouse inventory)
    orders.selectKey((id, order) -> order.getProduct())
        //Limit to newly created orders
        .filter((id, order) -> OrderState.CREATED.equals(order.getState()))
        //Join Orders to Inventory so we can compare each order to its corresponding stock value
        .join(warehouseInventory, KeyValue::new, Topics.WAREHOUSE_INVENTORY.keySerde(), Topics.ORDERS.valueSerde())
        //Validate the order based on how much stock we have both in the warehouse and locally 'reserved' stock
        .transform(InventoryValidator::new, RESERVED_STOCK_STORE_NAME)
        //Push the result into the Order Validations topic
        .to(Topics.ORDER_VALIDATIONS.keySerde(), Topics.ORDER_VALIDATIONS.valueSerde(),
            Topics.ORDER_VALIDATIONS.name());

    return new KafkaStreams(builder,
        MicroserviceUtils.baseStreamsConfig(bootstrapServers, stateDir, INVENTORY_SERVICE_APP_ID));
  }

  private static class InventoryValidator implements Transformer<Product, KeyValue<Order, Integer>, KeyValue<String, OrderValidation>> {

    private KeyValueStore<Product, Long> reservedStocksStore;

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
      reservedStocksStore = (KeyValueStore<Product, Long>) context
          .getStateStore(RESERVED_STOCK_STORE_NAME);
    }

    @Override
    public KeyValue<String, OrderValidation> transform(final Product productId,
        final KeyValue<Order, Integer> orderAndStock) {
      //Process each order/inventory pair one at a time
      OrderValidation validated;
      Order order = orderAndStock.key;
      Integer warehouseStockCount = orderAndStock.value;

      //Look up locally 'reserved' stock from our state store
      Long reserved = reservedStocksStore.get(order.getProduct());
      if (reserved == null) {
        reserved = 0L;
      }

      //If there is enough stock available (considering both warehouse inventory and reserved stock) validate the order
      if (warehouseStockCount - reserved - order.getQuantity() >= 0) {
        //reserve the stock by adding it to the 'reserved' store
        reservedStocksStore.put(order.getProduct(), reserved + order.getQuantity());
        //validate the order
        validated = new OrderValidation(order.getId(), INVENTORY_CHECK, PASS);
      } else {
        //fail the order
        validated = new OrderValidation(order.getId(), INVENTORY_CHECK, FAIL);
      }
      return KeyValue.pair(validated.getOrderId(), validated);
    }

    @Override
    public KeyValue<String, OrderValidation> punctuate(long timestamp) {
      return null;
    }

    @Override
    public void close() {
    }
  }

  public static void main(String[] args) throws Exception {
    InventoryService service = new InventoryService();
    service.start(parseArgsAndConfigure(args));
    addShutdownHookAndBlock(service);
  }
}