package com.tresata.spark.kafka

import java.net.ConnectException

import kafka.api.{FetchRequestBuilder, OffsetRequest, PartitionOffsetRequestInfo, TopicMetadataRequest}
import kafka.common.{BrokerNotAvailableException, ErrorMapping, LeaderNotAvailableException, NotLeaderForPartitionException, TopicAndPartition}
import kafka.consumer.SimpleConsumer
import kafka.message.{Message, MessageAndOffset, MessageSet}
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.slf4j.LoggerFactory

import scala.collection.immutable.SortedMap
import scala.util.Random

private object Broker {
  def apply(s: String): Broker = s.split(":") match {
    case Array(host) => Broker(host, 9092)
    case Array(host, port) => Broker(host, port.toInt)
  }
}

private case class Broker(host: String, port: Int) {
  override def toString: String = host + ":" + port
}

private class KafkaPartition(val rddId: Int, override val index: Int, val partition: Int, val startOffset: Long, val stopOffset: Long, val leader: Option[Broker])
  extends Partition {
  override def equals(other: Any): Boolean = other match {
    case kp: KafkaPartition => rddId == kp.rddId && index == kp.index
    case _ => false
  }

  override def hashCode: Int = 41 * (41 + rddId) + index
}

case class PartitionOffsetMessage(partition: Int, offset: Long, message: Message)

object KafkaRDD {
  private val log = LoggerFactory.getLogger(getClass)

  private def simpleConsumer(broker: Broker, config: SimpleConsumerConfig): SimpleConsumer =
    new SimpleConsumer(broker.host, broker.port, config.socketTimeoutMs, config.socketReceiveBufferBytes, config.clientId)

  private def partitionLeaders(topic: String, consumer: SimpleConsumer): Map[Int, Option[Broker]] = {
    val topicMeta = consumer.send(new TopicMetadataRequest(Seq(topic), 0)).topicsMetadata.head
    ErrorMapping.maybeThrowException(topicMeta.errorCode)
    topicMeta.partitionsMetadata.map{ partitionMeta =>
      ErrorMapping.maybeThrowException(partitionMeta.errorCode)
      (partitionMeta.partitionId, partitionMeta.leader.map{ b => Broker(b.host, b.port) })
    }.toMap
  }

  private def partitionLeaders(topic: String, brokers: Iterable[Broker], config: SimpleConsumerConfig): Map[Int, Option[Broker]] = {
    val it = Random.shuffle(brokers).take(5).iterator.flatMap{ broker =>
      val consumer = simpleConsumer(broker, config)
      try {
        Some(partitionLeaders(topic, consumer))
      } catch {
        case e: ConnectException =>
          log.warn("connection failed for broker {}", broker)
          None
      } finally {
        consumer.close()
      }
    }
    if (it.hasNext) it.next() else throw new BrokerNotAvailableException("operation failed for all brokers")
  }

  private def partitionOffset(tap: TopicAndPartition, time: Long, consumer: SimpleConsumer): Long = {
    val partitionOffsetsResponse = consumer.getOffsetsBefore(OffsetRequest(Map(tap -> PartitionOffsetRequestInfo(time, 1))))
      .partitionErrorAndOffsets(tap)
    ErrorMapping.maybeThrowException(partitionOffsetsResponse.error)
    partitionOffsetsResponse.offsets.head
  }

  private def partitionOffsets(topic: String, time: Long, leaders: Map[Int, Option[Broker]], config: SimpleConsumerConfig): Map[Int, Long] =
    leaders.par.map{
      case (partition, None) =>
        throw new LeaderNotAvailableException(s"no leader for partition ${partition}")
      case (partition, Some(leader)) =>
        val consumer = simpleConsumer(leader, config)
        try {
          (partition, partitionOffset(TopicAndPartition(topic, partition), time, consumer))
        } finally {
          consumer.close()
        }
    }.seq

  private def retryIfNoLeader[E](e: => E, config: SimpleConsumerConfig): E = {
    def sleep() {
      log.warn("sleeping for {} ms", config.refreshLeaderBackoffMs)
      Thread.sleep(config.refreshLeaderBackoffMs)
    }

    def attempt(e: => E, nr: Int = 1): E = if (nr < config.refreshLeaderMaxRetries) {
      try(e) catch {
        case ex: LeaderNotAvailableException => sleep(); attempt(e, nr + 1)
        case ex: NotLeaderForPartitionException => sleep(); attempt(e, nr + 1)
        case ex: ConnectException => sleep(); attempt(e, nr + 1)
      }
    } else e

    attempt(e)
  }

  private def brokerList(config: SimpleConsumerConfig): List[Broker] =
    config.metadataBrokerList.split(",").toList.map(Broker.apply)

  def apply(sc: SparkContext, topic: String, offsets: Map[Int, (Long, Long)], config: SimpleConsumerConfig): KafkaRDD =
    new KafkaRDD(sc, topic, offsets, config)

  def apply(sc: SparkContext, topic: String, startOffsets: Map[Int, Long], stopTime: Long, config: SimpleConsumerConfig): KafkaRDD = {
    val brokers = brokerList(config)
    val stopOffsets = retryIfNoLeader({
      val leaders = partitionLeaders(topic, brokers, config)
      partitionOffsets(topic, stopTime, leaders, config)
    }, config)
    require(stopOffsets.keySet.subsetOf(startOffsets.keySet),
      "missing start offset for partition(s) " + (stopOffsets.keySet -- startOffsets.keySet).toList.sorted.mkString(", ")
    )
    val offsets = stopOffsets.map{ case (partition, stopOffset) => (partition, (startOffsets(partition), stopOffset)) }
    new KafkaRDD(sc, topic, offsets, config)
  }

  def apply(sc: SparkContext, topic: String, startTime: Long, stopTime: Long, config: SimpleConsumerConfig): KafkaRDD = {
    val brokers = brokerList(config)
    val (startOffsets, stopOffsets) = retryIfNoLeader({
      val leaders = partitionLeaders(topic, brokers, config)
      (partitionOffsets(topic, startTime, leaders, config), partitionOffsets(topic, stopTime, leaders, config))
    }, config)
    val offsets = startOffsets.map{ case (partition, startOffset) => (partition, (startOffset, stopOffsets(partition))) }
    new KafkaRDD(sc, topic, offsets, config)
  }

  /** Write contents of this RDD to Kafka messages by creating a Producer per partition. */
  def writeWithKeysToKafka[K,V](rdd: RDD[(K,V)], topic: String, config: ProducerConfig) {
    val props = config.props.props

    def write(context: TaskContext, iter: Iterator[(K,V)]) {
      val config = new ProducerConfig(props)
      val producer = new Producer[K,V](config)
      try {
        iter.foreach{ case (key, msg) =>
          if (context.isInterrupted) sys.error("interrupted")
          producer.send(new KeyedMessage(topic, key, msg))
        }
      } finally {
        producer.close()
      }
    }
    rdd.context.runJob(rdd, write _)
  }

  def writeToKafka[V](rdd: RDD[V], topic: String, config: ProducerConfig) {
    writeWithKeysToKafka[V, V](rdd.map((null.asInstanceOf[V], _)), topic, config)
  }

  /** Returns two Maps (PartitionId -> Offset) containing smallest and largests offsets for each partition**/
  def smallestAndLargestOffsets(topic: String, config: SimpleConsumerConfig): (Map[Int, Long], Map[Int, Long]) = {
    val brokers = brokerList(config)

    retryIfNoLeader({
      val leaders = partitionLeaders(topic, brokers, config)

      (partitionOffsets(topic, OffsetRequest.EarliestTime, leaders, config),
        partitionOffsets(topic, OffsetRequest.LatestTime, leaders, config))
    }, config)
  }

}

class KafkaRDD private (sc: SparkContext, val topic: String, val offsets: Map[Int, (Long, Long)], config: SimpleConsumerConfig)
  extends RDD[PartitionOffsetMessage](sc, Nil) {
  import KafkaRDD._
  log.info("offsets {}", SortedMap(offsets.toSeq: _*).mkString(", "))

  def startOffsets: Map[Int, Long] = offsets.mapValues(_._1)
  def stopOffsets: Map[Int, Long] = offsets.mapValues(_._2)

  private val props = config.props.props

  private val brokers = brokerList(config)
  log.info("brokers {}", brokers.mkString(", "))

  private val leaders = partitionLeaders(topic, brokers, config)
  log.info("leaders {}", SortedMap(leaders.toSeq: _*).mapValues(_.map(_.toString).getOrElse("?")).mkString(", "))

  require(leaders.keySet.subsetOf(offsets.keySet),
    "missing offsets for partition(s) " + (leaders.keySet -- offsets.keySet).toList.sorted.mkString(", ")
  )

  protected def getPartitions: Array[Partition] = leaders.zipWithIndex.map{ case ((partition, leader), index) =>
    val (startOffset, stopOffset) = offsets(partition)
    new KafkaPartition(id, index, partition, startOffset, stopOffset, leader)
  }.toArray

  override def getPreferredLocations(split: Partition): Seq[String] = split.asInstanceOf[KafkaPartition].leader.map(_.host).toSeq

  def compute(split: Partition, context: TaskContext): Iterator[PartitionOffsetMessage] = {
    val kafkaSplit = split.asInstanceOf[KafkaPartition]
    val partition = kafkaSplit.partition
    val tap = TopicAndPartition(topic, partition)
    val startOffset = kafkaSplit.startOffset
    val stopOffset = kafkaSplit.stopOffset
    val config = SimpleConsumerConfig(props)
    log.info(s"partition ${partition} offset range [${startOffset}, ${stopOffset})")

    def sleep() {
      log.warn("sleeping for {} ms", config.refreshLeaderBackoffMs)
      Thread.sleep(config.refreshLeaderBackoffMs)
    }

    try {
      // every task reads from a single broker
      // on the first attempt we use the lead broker determined in the driver, on next attempts we ask for the lead broker ourselves
      val broker = (if (context.attemptNumber == 0) kafkaSplit.leader else None)
        .orElse(partitionLeaders(topic, brokers, config)(partition))
        .getOrElse(throw new LeaderNotAvailableException(s"no leader for partition ${partition}"))
      log.info("reading from leader {}", broker)

      // this is the consumer that reads from the broker
      // the consumer is always closed upon task completion
      val consumer = simpleConsumer(broker, config)
      context.addTaskCompletionListener(_ => consumer.close())

      def fetch(offset: Long): MessageSet = {
        log.debug("fetching with offset {}", offset)
        val req = new FetchRequestBuilder().clientId(config.clientId).addFetch(topic, partition, offset, config.fetchMessageMaxBytes).build
        val resp = consumer.fetch(req)
        val data = resp.data.toMap
        val partitionData = data(tap)
        ErrorMapping.maybeThrowException(partitionData.error)
        log.debug("fetched {} messages", partitionData.messages.size)
        partitionData.messages
      }

      new Iterator[MessageAndOffset] {
        private var offset = startOffset
        private var setIter = fetch(offset).iterator

        override def hasNext: Boolean = offset < stopOffset // blindly trusting kafka here

        override def next(): MessageAndOffset = {
          if (!setIter.hasNext)
            setIter = fetch(offset).iterator
          val x = setIter.next()
          offset = x.nextOffset // is there a nicer way?
          x
        }
      }
        .filter(_.offset >= startOffset) // is this necessary?
        .map{ mao => PartitionOffsetMessage(partition, mao.offset, mao.message) }
    } catch {
      case e: LeaderNotAvailableException => sleep(); throw e
      case e: NotLeaderForPartitionException => sleep(); throw e
      case e: ConnectException => sleep(); throw e
    }
  }
}