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
package io.confluent.examples.streams

import java.util.{Collections, Properties}

import io.confluent.examples.streams.kafka.EmbeddedSingleNodeKafkaCluster
import io.confluent.kafka.serializers.{AbstractKafkaAvroSerDeConfig, KafkaAvroDeserializer, KafkaAvroSerializer}
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams.kstream.{KStream, Produced}
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig}
import org.assertj.core.api.Assertions.assertThat
import org.junit._
import org.scalatest.junit.AssertionsForJUnit

/**
  * End-to-end integration test that demonstrates how to work on Generic Avro data.
  *
  * See [[SpecificAvroScalaIntegrationTest]] for the equivalent Specific Avro integration test.
  */
class GenericAvroScalaIntegrationTest extends AssertionsForJUnit {

  private val privateCluster: EmbeddedSingleNodeKafkaCluster = new EmbeddedSingleNodeKafkaCluster

  @Rule def cluster: EmbeddedSingleNodeKafkaCluster = privateCluster

  private val inputTopic = "inputTopic"
  private val outputTopic = "output-topic"

  @Before
  def startKafkaCluster() {
    cluster.createTopic(inputTopic, 2, 1)
    cluster.createTopic(outputTopic)
  }

  @Test
  def shouldRoundTripGenericAvroDataThroughKafka() {
    val schema: Schema = new Schema.Parser().parse(getClass.getResourceAsStream("/avro/io/confluent/examples/streams/wikifeed.avsc"))
    val record: GenericRecord = {
      val r = new GenericData.Record(schema)
      r.put("user", "alice")
      r.put("is_new", true)
      r.put("content", "lorem ipsum")
      r
    }
    val inputValues: Seq[GenericRecord] = Seq(record)

    //
    // Step 1: Configure and start the processor topology.
    //
    val builder: StreamsBuilder = new StreamsBuilder()

    val streamsConfiguration: Properties = {
      val p = new Properties()
      p.put(StreamsConfig.APPLICATION_ID_CONFIG, "generic-avro-scala-integration-test")
      p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers())
      p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray.getClass.getName)
      p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, classOf[GenericAvroSerde])
      p.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, cluster.schemaRegistryUrl)
      p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      p
    }

    // Write the input data as-is to the output topic.
    //
    // Normally, because a) we have already configured the correct default serdes for keys and
    // values and b) the types for keys and values are the same for both the input topic and the
    // output topic, we would only need to define:
    //
    //   builder.stream(inputTopic).to(outputTopic);
    //
    // However, in the code below we intentionally override the default serdes in `to()` to
    // demonstrate how you can construct and configure a generic Avro serde manually.
    val stringSerde: Serde[String] = Serdes.String
    val genericAvroSerde: Serde[GenericRecord] = {
      val gas = new GenericAvroSerde
      // Note how we must manually call `configure()` on this serde to configure the schema registry
      // url.  This is different from the case of setting default serdes (see `streamsConfiguration`
      // above), which will be auto-configured based on the `StreamsConfiguration` instance.
      val isKeySerde: Boolean = false
      gas.configure(Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, cluster.schemaRegistryUrl), isKeySerde)
      gas
    }

    val stream: KStream[String, GenericRecord] = builder.stream(inputTopic)
    stream.to(outputTopic, Produced.`with`(stringSerde, genericAvroSerde))
    val streams: KafkaStreams = new KafkaStreams(builder.build(), streamsConfiguration)
    streams.start()

    //
    // Step 2: Produce some input data to the input topic.
    //
    val producerConfig: Properties = {
      val p = new Properties()
      p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers())
      p.put(ProducerConfig.ACKS_CONFIG, "all")
      p.put(ProducerConfig.RETRIES_CONFIG, "0")
      p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer])
      p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer])
      p.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, cluster.schemaRegistryUrl)
      p
    }
    import collection.JavaConverters._
    IntegrationTestUtils.produceValuesSynchronously(inputTopic, inputValues.asJava, producerConfig)

    //
    // Step 3: Verify the application's output data.
    //
    val consumerConfig = {
      val p = new Properties()
      p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers())
      p.put(ConsumerConfig.GROUP_ID_CONFIG, "generic-avro-scala-integration-test-standard-consumer")
      p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer])
      p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[KafkaAvroDeserializer])
      p.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, cluster.schemaRegistryUrl)
      p
    }
    val actualValues: java.util.List[GenericRecord] =
      IntegrationTestUtils.waitUntilMinValuesRecordsReceived(consumerConfig, outputTopic, inputValues.size)
    streams.close()
    assertThat(actualValues).containsExactlyElementsOf(inputValues.asJava)
  }

}
