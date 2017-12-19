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

import java.util.Properties

import io.confluent.examples.streams.kafka.EmbeddedSingleNodeKafkaCluster
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams.kstream.{KStream, KTable, Produced, Serialized}
import org.apache.kafka.streams._
import org.apache.kafka.test.TestUtils
import org.assertj.core.api.Assertions.assertThat
import org.junit._
import org.scalatest.junit.AssertionsForJUnit

/**
  * End-to-end integration test that demonstrates how to perform a join between a KStream and a
  * KTable (think: KStream.leftJoin(KTable)), i.e. an example of a stateful computation.
  *
  * See StreamToTableJoinIntegrationTest for the equivalent Java example.
  *
  * Note: We intentionally use JUnit4 (wrapped by ScalaTest) for implementing this Scala integration
  * test so it is easier to compare this Scala code with the equivalent Java code at
  * StreamToTableJoinIntegrationTest.  One difference is that, to simplify the Scala/Junit integration, we
  * switched from BeforeClass (which must be `static`) to Before as well as from @ClassRule (which
  * must be `static` and `public`) to a workaround combination of `@Rule def` and a `private val`.
  */
class StreamToTableJoinScalaIntegrationTest extends AssertionsForJUnit {

  private val privateCluster: EmbeddedSingleNodeKafkaCluster = new EmbeddedSingleNodeKafkaCluster

  @Rule def cluster: EmbeddedSingleNodeKafkaCluster = privateCluster

  private val userClicksTopic = "user-clicks"
  private val userRegionsTopic = "user-regions"
  private val outputTopic = "output-topic"

  @Before
  def startKafkaCluster() {
    cluster.createTopic(userClicksTopic)
    cluster.createTopic(userRegionsTopic)
    cluster.createTopic(outputTopic)
  }

  @Test
  def shouldCountClicksPerRegion() {
    // Scala-Java interoperability: to convert between Scala's `Tuple2` and Streams' `KeyValue`.
    import KeyValueImplicits._

    // Input 1: Clicks per user (multiple records allowed per user).
    val userClicks: Seq[KeyValue[String, Long]] = Seq(
      ("alice", 13L),
      ("bob", 4L),
      ("chao", 25L),
      ("bob", 19L),
      ("dave", 56L),
      ("eve", 78L),
      ("alice", 40L),
      ("fang", 99L)
    )

    // Input 2: Region per user (multiple records allowed per user).
    val userRegions: Seq[KeyValue[String, String]] = Seq(
      ("alice", "asia"), /* Alice lived in Asia originally... */
      ("bob", "americas"),
      ("chao", "asia"),
      ("dave", "europe"),
      ("alice", "europe"), /* ...but moved to Europe some time later. */
      ("eve", "americas"),
      ("fang", "asia")
    )

    val expectedClicksPerRegion: Seq[KeyValue[String, Long]] = Seq(
      ("americas", 101L),
      ("europe", 109L),
      ("asia", 124L)
    )

    //
    // Step 1: Configure and start the processor topology.
    //
    val stringSerde: Serde[String] = Serdes.String()
    // We want to use `Long` (which refers to `scala.Long`) throughout this code.  However, Kafka
    // ships only with serdes for `java.lang.Long`.  The "trick" below works because there is no
    // `scala.Long` at runtime (in most cases, `scala.Long` is just the primitive `long`), and
    // because Scala converts between `long` and `java.lang.Long` automatically.
    //
    // If you want to see how you work from Scala against Kafka's (Java-based) Streams API without
    // using this trick, take a look at `WordCountScalaIntegrationTest` where we explicitly use
    // `java.lang.Long` (renamed to `JLong`).
    val longSerde: Serde[Long] = Serdes.Long().asInstanceOf[Serde[Long]]

    val streamsConfiguration: Properties = {
      val p = new Properties()
      p.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-table-join-scala-integration-test")
      p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers())
      p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
      p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
      // The commit interval for flushing records to state stores and downstream must be lower than
      // this integration test's timeout (30 secs) to ensure we observe the expected processing results.
      p.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "10000")
      p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      // Use a temporary directory for storing state, which will be automatically removed after the test.
      p.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory.getAbsolutePath)
      p
    }

    val builder: StreamsBuilder = new StreamsBuilder()

    // This KStream contains information such as "alice" -> 13L.
    //
    // Because this is a KStream ("record stream"), multiple records for the same user will be
    // considered as separate click-count events, each of which will be added to the total count.
    val userClicksStream: KStream[String, Long] = builder.stream(userClicksTopic, Consumed.`with`(stringSerde, longSerde))

    // This KTable contains information such as "alice" -> "europe".
    //
    // Because this is a KTable ("changelog stream"), only the latest value (here: region) for a
    // record key will be considered at the time when a new user-click record (see above) is
    // received for the `leftJoin` below.  Any previous region values are being considered out of
    // date.  This behavior is quite different to the KStream for user clicks above.
    //
    // For example, the user "alice" will be considered to live in "europe" (although originally she
    // lived in "asia") because, at the time her first user-click record is being received and
    // subsequently processed in the `leftJoin`, the latest region update for "alice" is "europe"
    // (which overrides her previous region value of "asia").
    val userRegionsTable: KTable[String, String] = builder.table(userRegionsTopic, Consumed.`with`(stringSerde, stringSerde))

    // Compute the number of clicks per region, e.g. "europe" -> 13L.
    //
    // The resulting KTable is continuously being updated as new data records are arriving in the
    // input KStream `userClicksStream` and input KTable `userRegionsTable`.
    val userClicksJoinRegion : KStream[String, (String, Long)] = userClicksStream
      // Join the stream against the table.
      //
      // Null values possible: In general, null values are possible for region (i.e. the value of
      // the KTable we are joining against) so we must guard against that (here: by setting the
      // fallback region "UNKNOWN").  In this specific example this is not really needed because
      // we know, based on the test setup, that all users have appropriate region entries at the
      // time we perform the join.
      .leftJoin(userRegionsTable, (clicks: Long, region: String) => (if (region == null) "UNKNOWN" else region, clicks))
    val clicksByRegion : KStream[String, Long] = userClicksJoinRegion
      // Change the stream from <user> -> <region, clicks> to <region> -> <clicks>
      .map((_: String, regionWithClicks: (String, Long)) => new KeyValue[String, Long](
      regionWithClicks._1, regionWithClicks._2))

    val clicksPerRegion: KTable[String, Long] = clicksByRegion
        // Compute the total per region by summing the individual click counts per region.
        .groupByKey(Serialized.`with`(stringSerde, longSerde))
        .reduce((firstClicks: Long, secondClicks: Long) => firstClicks + secondClicks: Long)

    // Write the (continuously updating) results to the output topic.
    clicksPerRegion.toStream().to(outputTopic, Produced.`with`(stringSerde, longSerde))

    val streams: KafkaStreams = new KafkaStreams(builder.build(), streamsConfiguration)
    streams.start()

    //
    // Step 2: Publish user-region information.
    //
    // To keep this code example simple and easier to understand/reason about, we publish all
    // user-region records before any user-click records (cf. step 3).  In practice though,
    // data records would typically be arriving concurrently in both input streams/topics.
    val userRegionsProducerConfig: Properties = {
      val p = new Properties()
      p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers())
      p.put(ProducerConfig.ACKS_CONFIG, "all")
      p.put(ProducerConfig.RETRIES_CONFIG, "0")
      p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
      p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
      p
    }
    import collection.JavaConverters._
    IntegrationTestUtils.produceKeyValuesSynchronously(userRegionsTopic, userRegions.asJava, userRegionsProducerConfig)

    //
    // Step 3: Publish some user click events.
    //
    val userClicksProducerConfig: Properties = {
      val p = new Properties()
      p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers())
      p.put(ProducerConfig.ACKS_CONFIG, "all")
      p.put(ProducerConfig.RETRIES_CONFIG, "0")
      p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
      p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[LongSerializer])
      p
    }
    IntegrationTestUtils.produceKeyValuesSynchronously(userClicksTopic, userClicks.asJava, userClicksProducerConfig)

    //
    // Step 4: Verify the application's output data.
    //
    val consumerConfig = {
      val p = new Properties()
      p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers())
      p.put(ConsumerConfig.GROUP_ID_CONFIG, "join-scala-integration-test-standard-consumer")
      p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
      p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[LongDeserializer])
      p
    }
    val actualClicksPerRegion: java.util.List[KeyValue[String, Long]] = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig,
      outputTopic, expectedClicksPerRegion.size)
    streams.close()
    assertThat(actualClicksPerRegion).containsExactlyElementsOf(expectedClicksPerRegion.asJava)
  }

}
