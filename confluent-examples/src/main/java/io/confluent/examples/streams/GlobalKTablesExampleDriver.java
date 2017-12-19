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
package io.confluent.examples.streams;

import io.confluent.examples.streams.avro.Customer;
import io.confluent.examples.streams.avro.EnrichedOrder;
import io.confluent.examples.streams.avro.Order;
import io.confluent.examples.streams.avro.Product;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import static io.confluent.examples.streams.GlobalKTablesExample.CUSTOMER_TOPIC;
import static io.confluent.examples.streams.GlobalKTablesExample.ENRICHED_ORDER_TOPIC;
import static io.confluent.examples.streams.GlobalKTablesExample.ORDER_TOPIC;
import static io.confluent.examples.streams.GlobalKTablesExample.PRODUCT_TOPIC;

/**
 * This is a sample driver for the {@link GlobalKTablesExample}.
 * To run this driver please first refer to the instructions in {@link GlobalKTablesExample}.
 * You can then run this class directly in your IDE or via the command line.
 * <p>
 * To run via the command line you might want to package as a fatjar first. Please refer to:
 * <a href='https://github.com/confluentinc/kafka-streams-examples#packaging-and-running'>Packaging</a>
 * <p>
 * Once packaged you can then run:
 * <pre>
 * {@code
 * $ java -cp target/kafka-streams-examples-4.0.0-SNAPSHOT-standalone.jar io.confluent.examples.streams.GlobalKTablesExampleDriver
 * }
 * </pre>
 */
public class GlobalKTablesExampleDriver {

  private static final Random RANDOM = new Random();
  private static final int RECORDS_TO_GENERATE = 100;

  public static void main(String[] args) {
    final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
    final String schemaRegistryUrl = args.length > 1 ? args[1] : "http://localhost:8081";
    generateCustomers(bootstrapServers, schemaRegistryUrl, RECORDS_TO_GENERATE);
    generateProducts(bootstrapServers, schemaRegistryUrl, RECORDS_TO_GENERATE);
    generateOrders(bootstrapServers, schemaRegistryUrl, RECORDS_TO_GENERATE, RECORDS_TO_GENERATE, RECORDS_TO_GENERATE);
    receiveEnrichedOrders(bootstrapServers, schemaRegistryUrl, RECORDS_TO_GENERATE);
  }

  private static void receiveEnrichedOrders(final String bootstrapServers,
                                                           final String schemaRegistryUrl,
                                                           final int expected) {
    final Properties consumerProps = new Properties();
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "global-tables-consumer");
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Serdes.Long().deserializer().getClass());
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
    consumerProps.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
    consumerProps.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

    final KafkaConsumer<Long, EnrichedOrder> consumer = new KafkaConsumer<>(consumerProps);
    consumer.subscribe(Collections.singleton(ENRICHED_ORDER_TOPIC));
    int received = 0;
    while(received < expected) {
      final ConsumerRecords<Long, EnrichedOrder> records = consumer.poll(Long.MAX_VALUE);
      records.forEach(record -> System.out.println(record.value()));
    }
    consumer.close();
  }

  static List<Customer> generateCustomers(final String bootstrapServers,
                                                 final String schemaRegistryUrl,
                                                 final int count) {

    final SpecificAvroSerde<Customer> customerSerde = createSerde(schemaRegistryUrl);
    final Properties producerProperties = new Properties();
    producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

    final KafkaProducer<Long, Customer>
        customerProducer =
        new KafkaProducer<>(producerProperties, Serdes.Long().serializer(), customerSerde.serializer());
    final List<Customer> allCustomers = new ArrayList<>();
    final String [] genders = {"male", "female", "unknown"};
    final Random random = new Random();
    for(long i = 0; i < count; i++) {
      final Customer customer = new Customer(randomString(10),
                                             genders[random.nextInt(genders.length)],
                                             randomString(20));
      allCustomers.add(customer);
      customerProducer.send(new ProducerRecord<>(CUSTOMER_TOPIC, i, customer));
    }
    customerProducer.close();
    return allCustomers;
  }

  static List<Product> generateProducts(final String bootstrapServers,
                                               final String schemaRegistryUrl,
                                               final int count) {

    final SpecificAvroSerde<Product> productSerde = createSerde(schemaRegistryUrl);

    final Properties producerProperties = new Properties();
    producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

    final KafkaProducer<Long, Product>
        producer =
        new KafkaProducer<>(producerProperties, Serdes.Long().serializer(), productSerde.serializer());
    final List<Product> allProducts = new ArrayList<>();
    for(long i = 0; i < count; i++) {
      final Product product = new Product(randomString(10),
                                          randomString(RECORDS_TO_GENERATE),
                                          randomString(20));
      allProducts.add(product);
      producer.send(new ProducerRecord<>(PRODUCT_TOPIC, i, product));
    }
    producer.close();
    return allProducts;
  }

  static List<Order> generateOrders(final String bootstrapServers,
                                             final String schemaRegistryUrl,
                                             final int numCustomers,
                                             final int numProducts,
                                             final int count) {

    final SpecificAvroSerde<Order> ordersSerde = createSerde(schemaRegistryUrl);

    final Properties producerProperties = new Properties();
    producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

    final KafkaProducer<Long, Order>
        producer =
        new KafkaProducer<>(producerProperties, Serdes.Long().serializer(), ordersSerde.serializer());

    final List<Order> allOrders = new ArrayList<>();
    for(long i = 0; i < count; i++) {
      final long customerId = RANDOM.nextInt(numCustomers);
      final long productId = RANDOM.nextInt(numProducts);
      final Order order = new Order(customerId,
                                    productId,
                                    RANDOM.nextLong());
      allOrders.add(order);
      producer.send(new ProducerRecord<>(ORDER_TOPIC, i, order));
    }
    producer.close();
    return allOrders;
  }

  private static <VT extends SpecificRecord> SpecificAvroSerde<VT> createSerde(final String schemaRegistryUrl) {

    final SpecificAvroSerde<VT> serde = new SpecificAvroSerde<>();
    final Map<String, String> serdeConfig = Collections.singletonMap(
        AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
    serde.configure(serdeConfig, false);
    return serde;
  }

  // Copied from org.apache.kafka.test.TestUtils
  private static String randomString(int len) {
    final StringBuilder b = new StringBuilder();

    for(int i = 0; i < len; ++i) {
      b.append("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789".charAt(RANDOM.nextInt
          ("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789".length())));
    }

    return b.toString();
  }

}
