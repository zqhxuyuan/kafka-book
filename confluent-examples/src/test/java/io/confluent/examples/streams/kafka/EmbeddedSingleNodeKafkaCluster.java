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
package io.confluent.examples.streams.kafka;

import io.confluent.examples.streams.zookeeper.ZooKeeperEmbedded;
import io.confluent.kafka.schemaregistry.RestApp;
import io.confluent.kafka.schemaregistry.avro.AvroCompatibilityLevel;
import java.io.IOException;
import java.util.Properties;
import kafka.server.KafkaConfig$;
import org.apache.curator.test.InstanceSpec;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs an in-memory, "embedded" Kafka cluster with 1 ZooKeeper instance, 1 Kafka broker, and 1
 * Confluent Schema Registry instance.
 */
public class EmbeddedSingleNodeKafkaCluster extends ExternalResource {

  private static final Logger log = LoggerFactory.getLogger(EmbeddedSingleNodeKafkaCluster.class);
  private static final int DEFAULT_BROKER_PORT = 0; // 0 results in a random port being selected
  private static final String KAFKA_SCHEMAS_TOPIC = "_schemas";
  private static final String AVRO_COMPATIBILITY_TYPE = AvroCompatibilityLevel.NONE.name;

  private ZooKeeperEmbedded zookeeper;
  private KafkaEmbedded broker;
  private RestApp schemaRegistry;
  private final Properties brokerConfig;
  private boolean running;

  /**
   * Creates and starts the cluster.
   */
  public EmbeddedSingleNodeKafkaCluster() {
    this(new Properties());
  }

  /**
   * Creates and starts the cluster.
   *
   * @param brokerConfig Additional broker configuration settings.
   */
  public EmbeddedSingleNodeKafkaCluster(Properties brokerConfig) {
    this.brokerConfig = new Properties();
    this.brokerConfig.putAll(brokerConfig);
  }

  /**
   * Creates and starts the cluster.
   */
  public void start() throws Exception {
    log.debug("Initiating embedded Kafka cluster startup");
    log.debug("Starting a ZooKeeper instance...");
    zookeeper = new ZooKeeperEmbedded();
    log.debug("ZooKeeper instance is running at {}", zookeeper.connectString());

    Properties effectiveBrokerConfig = effectiveBrokerConfigFrom(brokerConfig, zookeeper);
    log.debug("Starting a Kafka instance on port {} ...",
        effectiveBrokerConfig.getProperty(KafkaConfig$.MODULE$.PortProp()));
    broker = new KafkaEmbedded(effectiveBrokerConfig);
    log.debug("Kafka instance is running at {}, connected to ZooKeeper at {}",
        broker.brokerList(), broker.zookeeperConnect());

    schemaRegistry = new RestApp(
        InstanceSpec.getRandomPort(),
        zookeeperConnect(),
        KAFKA_SCHEMAS_TOPIC, AVRO_COMPATIBILITY_TYPE, new Properties());
    schemaRegistry.start();
    running = true;
  }

  private Properties effectiveBrokerConfigFrom(Properties brokerConfig, ZooKeeperEmbedded zookeeper) {
    Properties effectiveConfig = new Properties();
    effectiveConfig.putAll(brokerConfig);
    effectiveConfig.put(KafkaConfig$.MODULE$.ZkConnectProp(), zookeeper.connectString());
    effectiveConfig.put(KafkaConfig$.MODULE$.PortProp(), DEFAULT_BROKER_PORT);

    effectiveConfig.put(KafkaConfig$.MODULE$.DeleteTopicEnableProp(), true);
    effectiveConfig.put(KafkaConfig$.MODULE$.LogCleanerDedupeBufferSizeProp(), 2 * 1024 * 1024L);
    effectiveConfig.put(KafkaConfig$.MODULE$.GroupMinSessionTimeoutMsProp(), 0);
    effectiveConfig.put(KafkaConfig$.MODULE$.OffsetsTopicReplicationFactorProp(), (short) 1);
    effectiveConfig.put(KafkaConfig$.MODULE$.AutoCreateTopicsEnableProp(), true);
    return effectiveConfig;
  }

  @Override
  protected void before() throws Exception {
    start();
  }

  @Override
  protected void after() {
    stop();
  }

  /**
   * Stops the cluster.
   */
  public void stop() {
    log.info("Stopping Confluent");
    try {
      try {
        if (schemaRegistry != null) {
          schemaRegistry.stop();
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      if (broker != null) {
        broker.stop();
      }
      try {
        if (zookeeper != null) {
          zookeeper.stop();
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } finally {
      running = false;
    }
    log.info("Confluent Stopped");
  }

  /**
   * This cluster's `bootstrap.servers` value.  Example: `127.0.0.1:9092`.
   *
   * You can use this to tell Kafka Streams applications, Kafka producers, and Kafka consumers (new
   * consumer API) how to connect to this cluster.
   */
  public String bootstrapServers() {
    return broker.brokerList();
  }

  /**
   * This cluster's ZK connection string aka `zookeeper.connect` in `hostnameOrIp:port` format.
   * Example: `127.0.0.1:2181`.
   *
   * You can use this to e.g. tell Kafka consumers (old consumer API) how to connect to this
   * cluster.
   */
  public String zookeeperConnect() {
    return zookeeper.connectString();
  }

  /**
   * The "schema.registry.url" setting of the schema registry instance.
   */
  public String schemaRegistryUrl() {
    return schemaRegistry.restConnect;
  }

  /**
   * Creates a Kafka topic with 1 partition and a replication factor of 1.
   *
   * @param topic The name of the topic.
   */
  public void createTopic(String topic) {
    createTopic(topic, 1, 1, new Properties());
  }

  /**
   * Creates a Kafka topic with the given parameters.
   *
   * @param topic       The name of the topic.
   * @param partitions  The number of partitions for this topic.
   * @param replication The replication factor for (the partitions of) this topic.
   */
  public void createTopic(String topic, int partitions, int replication) {
    createTopic(topic, partitions, replication, new Properties());
  }

  /**
   * Creates a Kafka topic with the given parameters.
   *
   * @param topic       The name of the topic.
   * @param partitions  The number of partitions for this topic.
   * @param replication The replication factor for (partitions of) this topic.
   * @param topicConfig Additional topic-level configuration settings.
   */
  public void createTopic(String topic,
                          int partitions,
                          int replication,
                          Properties topicConfig) {
    broker.createTopic(topic, partitions, replication, topicConfig);
  }

  public boolean isRunning() {
    return running;
  }
}