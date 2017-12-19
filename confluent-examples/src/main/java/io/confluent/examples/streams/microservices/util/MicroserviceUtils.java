package io.confluent.examples.streams.microservices.util;

import io.confluent.examples.streams.avro.microservices.Product;
import io.confluent.examples.streams.microservices.Service;
import io.confluent.examples.streams.microservices.domain.Schemas;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.rocksdb.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MicroserviceUtils {

  private static final Logger log = LoggerFactory.getLogger(MicroserviceUtils.class);
  private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
  private static final String DEFAULT_SCHEMA_REGISTRY_URL = "http://localhost:8081";
  public static final long MIN = 60 * 1000L;

  public static int randomFreeLocalPort() throws IOException {
    ServerSocket s = new ServerSocket(0);
    int port = s.getLocalPort();
    s.close();
    return port;
  }

  public static String parseArgsAndConfigure(String[] args) {
    if (args.length > 2) {
      throw new IllegalArgumentException("usage: ... " +
          "[<bootstrap.servers> (optional, default: " + DEFAULT_BOOTSTRAP_SERVERS + ")] " +
          "[<schema.registry.url> (optional, default: " + DEFAULT_SCHEMA_REGISTRY_URL + ")] ");
    }
    final String bootstrapServers = args.length > 1 ? args[1] : "localhost:9092";
    final String schemaRegistryUrl = args.length > 2 ? args[2] : "http://localhost:8081";

    log.info("Connecting to Kafka cluster via bootstrap servers " + bootstrapServers);
    log.info("Connecting to Confluent schema registry at " + schemaRegistryUrl);
    Schemas.configureSerdesWithSchemaRegistryUrl(schemaRegistryUrl);
    return bootstrapServers;
  }

  public static Properties baseStreamsConfig(String bootstrapServers, String stateDir,
      String appId) {
    Properties config = new Properties();
    // Workaround for a known issue with RocksDB in environments where you have only 1 cpu core.
    config.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, CustomRocksDBConfig.class);
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    config.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "exactly_once");
    config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1); //commit as fast as possible
    return config;
  }

  public static class CustomRocksDBConfig implements RocksDBConfigSetter {

    @Override
    public void setConfig(final String storeName, final Options options,
        final Map<String, Object> configs) {
      // Workaround: We must ensure that the parallelism is set to >= 2.  There seems to be a known
      // issue with RocksDB where explicitly setting the parallelism to 1 causes issues (even though
      // 1 seems to be RocksDB's default for this configuration).
      int compactionParallelism = Math.max(Runtime.getRuntime().availableProcessors(), 2);
      // Set number of compaction threads (but not flush threads).
      options.setIncreaseParallelism(compactionParallelism);
    }
  }

  //Streams doesn't provide an Enum serdes so just create one here.
  public static final class ProductTypeSerde implements Serde<Product> {

    @Override
    public void configure(Map<String, ?> map, boolean b) {
    }

    @Override
    public void close() {
    }

    @Override
    public Serializer<Product> serializer() {
      return new Serializer<Product>() {
        @Override
        public void configure(Map<String, ?> map, boolean b) {
        }

        @Override
        public byte[] serialize(String topic, Product pt) {
          return pt.toString().getBytes();
        }

        @Override
        public void close() {
        }
      };
    }

    @Override
    public Deserializer<Product> deserializer() {
      return new Deserializer<Product>() {
        @Override
        public void configure(Map<String, ?> map, boolean b) {
        }

        @Override
        public Product deserialize(String topic, byte[] bytes) {
          return Product.valueOf(new String(bytes));
        }

        @Override
        public void close() {
        }
      };
    }
  }

  public static void setTimeout(long timeout, AsyncResponse asyncResponse) {
    asyncResponse.setTimeout(timeout, TimeUnit.MILLISECONDS);
    asyncResponse.setTimeoutHandler(resp -> resp.resume(
        Response.status(Response.Status.GATEWAY_TIMEOUT)
            .entity("HTTP GET timed out after " + timeout + " ms\n")
            .build()));
  }

  public static Server startJetty(int port, Object binding) {
    ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
    context.setContextPath("/");

    Server jettyServer = new Server(port);
    jettyServer.setHandler(context);

    ResourceConfig rc = new ResourceConfig();
    rc.register(binding);
    rc.register(JacksonFeature.class);

    ServletContainer sc = new ServletContainer(rc);
    ServletHolder holder = new ServletHolder(sc);
    context.addServlet(holder, "/*");

    try {
      jettyServer.start();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    log.info("Listening on " + jettyServer.getURI());
    return jettyServer;
  }

  public static <T> KafkaProducer startProducer(String bootstrapServers,
      Schemas.Topic<String, T> topic) {
    Properties producerConfig = new Properties();
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    producerConfig.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
    producerConfig.put(ProducerConfig.RETRIES_CONFIG, String.valueOf(Integer.MAX_VALUE));
    producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");

    return new KafkaProducer<>(producerConfig,
        topic.keySerde().serializer(),
        topic.valueSerde().serializer());
  }

  public static void addShutdownHookAndBlock(Service service) throws InterruptedException {
    Thread.currentThread().setUncaughtExceptionHandler((t, e) -> service.stop());
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        service.stop();
      } catch (Exception ignored) {
      }
    }));
    Thread.currentThread().join();
  }
}