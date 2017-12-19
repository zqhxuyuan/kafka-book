package io.confluent.examples.streams.microservices.domain;

import static io.confluent.examples.streams.microservices.util.MicroserviceUtils.ProductTypeSerde;
import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

import io.confluent.examples.streams.avro.microservices.Customer;
import io.confluent.examples.streams.avro.microservices.Order;
import io.confluent.examples.streams.avro.microservices.OrderValidation;
import io.confluent.examples.streams.avro.microservices.OrderValue;
import io.confluent.examples.streams.avro.microservices.Payment;
import io.confluent.examples.streams.avro.microservices.Product;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

/**
 * A utility class that represents Topics and their various Serializers/Deserializers in a
 * convenient form.
 */
public class Schemas {

  public static String schemaRegistryUrl = "";
  public static SpecificAvroSerde<OrderValue> ORDER_VALUE_SERDE = new SpecificAvroSerde<>();

  public static class Topic<K, V> {

    private String name;
    private Serde<K> keySerde;
    private Serde<V> valueSerde;

    Topic(String name, Serde<K> keySerde, Serde<V> valueSerde) {
      this.name = name;
      this.keySerde = keySerde;
      this.valueSerde = valueSerde;
      Topics.ALL.put(name, this);
    }

    public Serde<K> keySerde() {
      return keySerde;
    }

    public Serde<V> valueSerde() {
      return valueSerde;
    }

    public String name() {
      return name;
    }

    public String toString() {
      return name;
    }
  }

  public static class Topics {

    public static Map<String, Topic> ALL = new HashMap<>();
    public static Topic<String, Order> ORDERS;
    public static Topic<String, Payment> PAYMENTS;
    public static Topic<Long, Customer> CUSTOMERS;
    public static Topic<Product, Integer> WAREHOUSE_INVENTORY;
    public static Topic<String, OrderValidation> ORDER_VALIDATIONS;

    static {
      createTopics();
    }

    private static void createTopics() {
      ORDERS = new Topic<>("orders", Serdes.String(), new SpecificAvroSerde<Order>());
      PAYMENTS = new Topic<>("payments", Serdes.String(), new SpecificAvroSerde<Payment>());
      CUSTOMERS = new Topic<>("customers", Serdes.Long(), new SpecificAvroSerde<Customer>());
      ORDER_VALIDATIONS = new Topic<>("order-validations", Serdes.String(),
          new SpecificAvroSerde<OrderValidation>());
      WAREHOUSE_INVENTORY = new Topic<>("warehouse-inventory", new ProductTypeSerde(),
          Serdes.Integer());
      ORDER_VALUE_SERDE = new SpecificAvroSerde<>();
    }
  }

  public static void configureSerdesWithSchemaRegistryUrl(String url) {
    Topics.createTopics(); //wipe cached schema registry
    for (Topic topic : Topics.ALL.values()) {
      configure(topic.keySerde(), url);
      configure(topic.valueSerde(), url);
    }
    configure(ORDER_VALUE_SERDE, url);
    schemaRegistryUrl = url;
  }

  private static void configure(Serde serde, String url) {
    if (serde instanceof SpecificAvroSerde) {
      serde.configure(Collections.singletonMap(SCHEMA_REGISTRY_URL_CONFIG, url), false);
    }
  }
}