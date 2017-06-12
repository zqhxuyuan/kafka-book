package com.tresata.spark.kafka

import java.io.{File, IOException}
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.util.{Properties, UUID}

import kafka.admin.AdminUtils
import kafka.api.OffsetRequest
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import kafka.serializer.StringEncoder
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.{ZkUtils, ZKStringSerializer}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.zookeeper.server.{NIOServerCnxnFactory, ZooKeeperServer}
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, FunSpec}

import scala.concurrent.duration._

class KafkaRDDSpec extends FunSpec with Eventually with BeforeAndAfterAll {
  import KafkaTestUtils._

  val zkConnect = "localhost:2181"
  val zkConnectionTimeout = 6000
  val zkSessionTimeout = 6000

  val brokerPort = 9092
  val brokerProps = getBrokerConfig(brokerPort, zkConnect)
  val brokerConf = new KafkaConfig(brokerProps)

  val sparkConf = new SparkConf(false)
    .setMaster("local")
    .setAppName("test")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.ui.enabled", "false")

  protected var zookeeper: EmbeddedZookeeper = _
  protected var zkClient: ZkClient = _
  protected var zkUtils: ZkUtils = _
  protected var server: KafkaServer = _
  protected var sc: SparkContext = _

  override def beforeAll {
    // zookeeper server startup
    zookeeper = new EmbeddedZookeeper(zkConnect)
    //zkClient = new ZkClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, ZKStringSerializer)
    zkUtils = ZkUtils(zkConnect, zkSessionTimeout, zkConnectionTimeout, false)

    // kafka broker startup
    server = new KafkaServer(brokerConf)
    server.startup()
    Thread.sleep(2000)

    // spark
    sc = new SparkContext(sparkConf)
  }

  override def afterAll {
    sc.stop()

    server.shutdown()
    brokerConf.logDirs.foreach { f => deleteRecursively(new File(f)) }

    zkClient.close()
    zookeeper.shutdown()
  }

  describe("KafkaRDD") {
    it("should post messages to kafka") {
      val topic = "topic1"
      createTopic(topic)

      val rdd = sc.parallelize(List(("1", "a"), ("2", "b")))
      KafkaRDD.writeWithKeysToKafka(rdd, topic, new ProducerConfig(getProducerConfig(brokerConf.hostName + ":" + brokerConf.port)))
      Thread.sleep(1000)
    }

    it("should post messages to kafka without keys") {
      val topic = "topic1"

      val rdd = sc.parallelize(List("b", "c", "c", "c"))
      KafkaRDD.writeToKafka(rdd, topic, new ProducerConfig(getProducerConfig(brokerConf.hostName + ":" + brokerConf.port)))
      Thread.sleep(1000)
    }

    it("should collect the correct messages without provided offsets") {
      val topic = "topic1"

      val rdd = KafkaRDD(sc, topic, OffsetRequest.EarliestTime, OffsetRequest.LatestTime, SimpleConsumerConfig(getConsumerConfig(brokerPort)))
      assert(rdd.collect.map(pomToTuple).toList ===
        List((0, 0L, "1", "a"), (0, 1L, "2", "b"), (0, 2L, null, "b"), (0, 3L, null, "c"), (0, 4L, null, "c"), (0, 5L, null, "c")))
      assert(rdd.stopOffsets === Map(0 -> 6L))
    }

    it("should collect the correct messages with provided offsets") {
      val topic = "topic1"
      val sent = Map("e" -> 2)
      produceAndSendMessage(topic, sent)

      val rdd = KafkaRDD(sc, topic, Map(0 -> 6L), OffsetRequest.LatestTime, SimpleConsumerConfig(getConsumerConfig(brokerPort)))
      assert(rdd.collect.map(pomToTuple).toList === List((0, 6L, null, "e"), (0, 7L, null, "e")))
      assert(rdd.stopOffsets === Map(0 -> 8L))
    }

    it("should collect the correct messages if more than fetchSize") {
      val topic = "topic1"
      val send = Map("a" -> 101)
      produceAndSendMessage(topic, send)

      val rdd = KafkaRDD(sc, topic, Map(0 -> 8L), OffsetRequest.LatestTime, SimpleConsumerConfig(getConsumerConfig(brokerPort)))
      val l = rdd.collect.map(pomToTuple).toList
      assert(l.size == 101 && l.forall(_._4 == "a") && l.last == (0, 108L, null, "a"))
      assert(rdd.stopOffsets === Map(0 -> 109L))
    }

    it("should collect no messages if stop offset is equal to or less than start offset") {
      val topic = "topic1"
      val rdd = KafkaRDD(sc, topic, OffsetRequest.LatestTime, OffsetRequest.LatestTime, SimpleConsumerConfig(getConsumerConfig(brokerPort)))
      assert(rdd.count == 0)
    }
  }

  private val pomToTuple = { (pom: PartitionOffsetMessage) =>
    (pom.partition, pom.offset, byteBufferToString(pom.message.key), byteBufferToString(pom.message.payload))
  }

  private def byteBufferToString(bb: ByteBuffer): String = {
    if (bb == null) null
    else {
      val b = new Array[Byte](bb.remaining)
      bb.get(b, 0, b.length)
      new String(b, "UTF8")
    }
  }

  private def createTestMessage(topic: String, sent: Map[String, Int])
  : Seq[KeyedMessage[String, String]] = {
    (for ((s, freq) <- sent; i <- 0 until freq) yield {
      new KeyedMessage[String, String](topic, s)
    }).toSeq
  }

  def createTopic(topic: String) {
    //AdminUtils.createTopic(zkClient, topic, 1, 1)
    AdminUtils.createTopic(zkUtils, topic, 1, 1)
    waitUntilMetadataIsPropagated(Seq(server), topic, 0)
  }

  def produceAndSendMessage(topic: String, sent: Map[String, Int]) {
    val brokerAddr = brokerConf.hostName + ":" + brokerConf.port
    val producer = new Producer[String, String](new ProducerConfig(getProducerConfig(brokerAddr)))
    producer.send(createTestMessage(topic, sent): _*)
    producer.close()
    Thread.sleep(1000)
  }

  def waitUntilMetadataIsPropagated(servers: Seq[KafkaServer], topic: String, partition: Int): Int = {
    var leader: Int = -1
    eventually(timeout(1000 milliseconds), interval(100 milliseconds)) {
      assert(servers.foldLeft(true) {
        (result, server) =>
          val partitionStateOpt = server.apis.metadataCache.getPartitionInfo(topic, partition)
          partitionStateOpt match {
            case None => false
            case Some(partitionState) =>
              leader = partitionState.leaderIsrAndControllerEpoch.leaderAndIsr.leader
              result && leader >= 0 // is valid broker id
          }
      }, s"Partition [$topic, $partition] metadata not propagated after timeout")
    }
    leader
  }
}

object KafkaTestUtils {
  def getBrokerConfig(port: Int, zkConnect: String): Properties = {
    val props = new Properties()
    props.put("broker.id", "0")
    props.put("host.name", "localhost")
    props.put("port", port.toString)
    props.put("log.dir", createTempDir().getAbsolutePath)
    props.put("zookeeper.connect", zkConnect)
    props.put("log.flush.interval.messages", "1")
    props.put("replica.socket.timeout.ms", "1500")
    props
  }

  def getProducerConfig(brokerList: String): Properties = {
    val props = new Properties()
    props.put("metadata.broker.list", brokerList)
    props.put("serializer.class", classOf[StringEncoder].getName)
    props
  }

  def getConsumerConfig(port: Int): Properties = {
    val props = new Properties()
    props.put("metadata.broker.list", s"localhost:${port}")
    props.put("fetch.message.max.bytes", "100")
    props
  }

  def deleteRecursively(file: File) {
    if (file != null) {
      if ((file.isDirectory) && !isSymlink(file)) {
        for (child <- listFilesSafely(file)) {
          deleteRecursively(child)
        }
      }
      if (!file.delete()) {
        // Delete can also fail if the file simply did not exist
        if (file.exists()) {
          throw new IOException("Failed to delete: " + file.getAbsolutePath)
        }
      }
    }
  }

  def isSymlink(file: File): Boolean = {
    if (file == null) throw new NullPointerException("File must not be null")
    val fileInCanonicalDir = if (file.getParent() == null) {
      file
    } else {
      new File(file.getParentFile().getCanonicalFile(), file.getName())
    }

    if (fileInCanonicalDir.getCanonicalFile().equals(fileInCanonicalDir.getAbsoluteFile())) {
      return false
    } else {
      return true
    }
  }

  private def listFilesSafely(file: File): Seq[File] = {
    val files = file.listFiles()
    if (files == null) {
      throw new IOException("Failed to list files for dir: " + file)
    }
    files
  }

  def createTempDir(root: String = System.getProperty("java.io.tmpdir")): File = {
    var attempts = 0
    val maxAttempts = 10
    var dir: File = null
    while (dir == null) {
      attempts += 1
      if (attempts > maxAttempts) {
        throw new IOException("Failed to create a temp directory (under " + root + ") after " +
          maxAttempts + " attempts!")
      }
      try {
        dir = new File(root, "spark-" + UUID.randomUUID.toString)
        if (dir.exists() || !dir.mkdirs()) {
          dir = null
        }
      } catch { case e: IOException => ; }
    }
    dir
  }

  class EmbeddedZookeeper(val zkConnect: String) {
    val snapshotDir = createTempDir()
    val logDir = createTempDir()

    val zookeeper = new ZooKeeperServer(snapshotDir, logDir, 500)
    val (ip, port) = {
      val splits = zkConnect.split(":")
      (splits(0), splits(1).toInt)
    }
    val factory = new NIOServerCnxnFactory()
    factory.configure(new InetSocketAddress(ip, port), 16)
    factory.startup(zookeeper)

    val actualPort = factory.getLocalPort

    def shutdown() {
      factory.shutdown()
      deleteRecursively(snapshotDir)
      deleteRecursively(logDir)
    }
  }
}