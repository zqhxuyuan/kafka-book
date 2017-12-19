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

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

/**
 * Demonstrates how to configure Kafka Streams for secure stream processing.
 * <p>
 * This example showcases how to perform secure stream processing by configuring a Kafka Streams
 * application to 1. encrypt data-in-transit when communicating with its target Kafka cluster and 2.
 * enable client authentication (i.e. the Kafka Streams application authenticates itself to the
 * Kafka brokers).  The actual stream processing of the application is trivial and not the focus in
 * this specific example: the application will simply write its input data as-is to an output
 * topic.
 * <p>
 * <br>
 * HOW TO RUN THIS EXAMPLE
 * <p>
 * This example requires running a secure Kafka cluster. Because setting up such a secure cluster
 * is a bit more involved, we will use https://github.com/confluentinc/securing-kafka-blog, which
 * provides a pre-configured virtual machine that is deployed via Vagrant.
 * <p>
 * Tip: The configuration of this VM follows the instructions at <a href="http://www.confluent.io/blog/apache-kafka-security-authorization-authentication-encryption">Apache Kafka Security 101</a>.
 * We recommend to read this article as well as <a href="http://docs.confluent.io/current/kafka/security.html">Kafka Security</a>
 * to understand how you can install a secure Kafka cluster yourself.
 * <p>
 * 1) Start a secure ZooKeeper instance and a secure Kafka broker.
 * <p>
 * Follow the README instructions at <a href="https://github.com/confluentinc/securing-kafka-blog">https://github.com/confluentinc/securing-kafka-blog</a>.
 * <p>
 * You must follow the instructions all the way until you have started secure ZooKeeper and secure
 * Kafka via the command {@code sudo /usr/sbin/start-zk-and-kafka} from within the VM.
 * <p>
 * At this point you have a VM running, and inside this VM runs a secure single-node Kafka cluster.
 * <p>
 * 2) Within the VM: create the input and output topics used by this example.
 * <pre>
 * {@code
 * # If you haven't done so already, connect from your host machine (e.g. your laptop) to the
 * # Vagrant VM
 * $ vagrant ssh default
 *
 * # Secure ZooKeeper as configured in the VM requires SASL authentication
 * [vagrant@kafka ~]$ export KAFKA_OPTS="-Djava.security.auth.login.config=/etc/kafka/kafka_server_jaas.conf"
 *
 * # Create the topics `secure-input` and `secure-output`.
 * # See comment after the code box in case you run into an authentication failure.
 * [vagrant@kafka ~]$ kafka-topics --create --topic secure-input \
 *                                 --zookeeper localhost:2181 --partitions 1 --replication-factor 1
 * [vagrant@kafka ~]$ kafka-topics --create --topic secure-output \
 *                                 --zookeeper localhost:2181 --partitions 1 --replication-factor 1
 * }</pre>
 * Note on "authentication failure": If you attempt to create a topic right after you started
 * ZooKeeper and Kafka via {@code sudo /usr/sbin/start-zk-and-kafka}, you may temporarily run into the
 * following error:
 * <pre>
 * {@code
 * ERROR An error: (java.security.PrivilegedActionException: javax.security.sasl.SaslException:
 *          GSS initiate failed [Caused by GSSException: No valid credentials provided
 *          (Mechanism level: Server not found in Kerberos database (7) - LOOKING_UP_SERVER)])
 *   occurred when evaluating Zookeeper Quorum Member's  received SASL token.
 *   Zookeeper Client will go to AUTH_FAILED state.
 * Exception in thread "main" org.I0Itec.zkclient.exception.ZkAuthFailedException: Authentication
 * failure
 * }</pre>
 * If this happens, just wait a minute so that ZooKeeper can finish its startup, then try again.
 * <p>
 * 3) Within the VM: build this example application.
 * <p>
 * Once packaged you can then run:
 * <pre>
 * {@code
 * [vagrant@kafka ~]$ git clone https://github.com/confluentinc/kafka-streams-examples.git
 * [vagrant@kafka ~]$ cd kafka-streams-examples
 * [vagrant@kafka ~]$ git checkout master
 *
 * # Build and package the examples.  We skip the test suite because running the test suite
 * # requires more main memory than is available to the Vagrant VM by default.
 * [vagrant@kafka ~]$ mvn clean -DskipTests=true package
 *
 * # Now we can start this example application
 * [vagrant@kafka ~]$ java -cp target/kafka-streams-examples-4.0.0-SNAPSHOT-standalone.jar \
 *                             io.confluent.examples.streams.SecureKafkaStreamsExample
 * }
 * </pre>
 * 4) Write some input data to the source topic (e.g. via {@code kafka-console-producer}). The already
 * running example application (step 3) will automatically process this input data and write the
 * results as-is (i.e. unmodified) to the output topic.
 * <pre>
 * {@code
 * # Start the console producer. You can then enter input data by writing some line of text, followed by ENTER.
 * #
 * #   kafka streams<ENTER>
 * #   ships with<ENTER>
 * #   important security features<ENTER>
 * #
 * # Every line you enter will become the value of a single Kafka message.
 * $ kafka-console-producer --broker-list localhost:9093 --topic secure-input \
 *                          --producer.config /etc/kafka/producer_ssl.properties
 * }
 * </pre>
 * 5) Inspect the resulting data in the output topic, e.g. via {@code kafka-console-consumer}.
 * <pre>
 * {@code
 * $ kafka-console-consumer --topic secure-output --from-beginning \
 *                          --new-consumer --bootstrap-server localhost:9093 \
 *                          --consumer.config /etc/kafka/consumer_ssl.properties
 * }
 * </pre>
 * You should see output data similar to:
 * <pre>
 * {@code
 * kafka streams
 * ships with
 * important security features
 * }
 * </pre>
 * 6) Once you're done with your experiments, you can stop this example via {@code Ctrl-C}.
 * <p>
 * If you also want to shut down the secure ZooKeeper and Kafka instances, please follow the README
 * instructions at <a href="https://github.com/confluentinc/securing-kafka-blog">https://github.com/confluentinc/securing-kafka-blog</a>.
 */
public class SecureKafkaStreamsExample {

  public static void main(final String[] args) throws Exception {
    final String secureBootstrapServers = args.length > 0 ? args[0] : "localhost:9093";
    final Properties streamsConfiguration = new Properties();
    // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
    // against which the application is run.
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "secure-kafka-streams-app");
    streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "secure-kafka-streams-app-client");
    // Where to find secure (!) Kafka broker(s).  In the VM, the broker listens on port 9093 for
    // SSL connections.
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, secureBootstrapServers);
    // Specify default (de)serializers for record keys and for record values.
    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
    // Security settings.
    // 1. These settings must match the security settings of the secure Kafka cluster.
    // 2. The SSL trust store and key store files must be locally accessible to the application.
    //    Typically, this means they would be installed locally in the client machine (or container)
    //    on which the application runs.  To simplify running this example, however, these files
    //    were generated and stored in the VM in which the secure Kafka broker is running.  This
    //    also explains why you must run this example application from within the VM.
    streamsConfiguration.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
    streamsConfiguration.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "/etc/security/tls/kafka.client.truststore.jks");
    streamsConfiguration.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "test1234");
    streamsConfiguration.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "/etc/security/tls/kafka.client.keystore.jks");
    streamsConfiguration.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "test1234");
    streamsConfiguration.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, "test1234");

    final StreamsBuilder builder = new StreamsBuilder();
    // Write the input data as-is to the output topic.
    builder.stream("secure-input").to("secure-output");
    final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
    // Always (and unconditionally) clean local state prior to starting the processing topology.
    // We opt for this unconditional call here because this will make it easier for you to play around with the example
    // when resetting the application for doing a re-run (via the Application Reset Tool,
    // http://docs.confluent.io/current/streams/developer-guide.html#application-reset-tool).
    //
    // The drawback of cleaning up local state prior is that your app must rebuilt its local state from scratch, which
    // will take time and will require reading all the state-relevant data from the Kafka cluster over the network.
    // Thus in a production scenario you typically do not want to clean up always as we do here but rather only when it
    // is truly needed, i.e., only under certain conditions (e.g., the presence of a command line flag for your app).
    // See `ApplicationResetExample.java` for a production-like example.
    streams.cleanUp();
    streams.start();

    // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
      @Override
      public void run() {
        streams.close();
      }
    }));
  }

}
