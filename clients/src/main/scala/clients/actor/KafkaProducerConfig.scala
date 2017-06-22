package clients.actor

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer.ProducerConfig

/**
  * Created by ctao on 16-1-25.
  * kafka生产者配置信息
  */
trait KafkaProducerConfig extends Properties {

  import KafkaProducerConfig._

  private val producerPrefixWithDot = producerPrefix + "."

  private val allKeys = Seq(
    brokers,
    brokers,
    keySerializer,
    valueSerializer,
    partitioner,
    requiredAcks,
    servers
  )

  lazy val conf = ConfigFactory.load()
  allKeys.map { key ⇒
    if (conf.hasPath(key)) {
      put(key.replace(producerPrefixWithDot, ""), conf.getString(key))
    }
  }




}

object KafkaProducerConfig {

  val producerPrefix = "producer"


  //Producer Keys
  val brokers = s"$producerPrefix.metadata.broker.list"
  val keySerializer = s"$producerPrefix.key.com.linewell.akkakafka.common.serializer"
  val valueSerializer = s"$producerPrefix.value.com.linewell.akkakafka.common.serializer"
  val servers = s"$producerPrefix.bootstrap.servers"
  val partitioner = s"$producerPrefix.partitioner.class"
  val requiredAcks = s"$producerPrefix.request.required.acks"


  def apply(): KafkaProducerConfig = new KafkaProducerConfig {}

}