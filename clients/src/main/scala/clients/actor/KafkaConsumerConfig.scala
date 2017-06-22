package clients.actor

import java.util.Properties

import com.typesafe.config.ConfigFactory

/**
  * Created by ctao on 16-1-27.
  * kafka消费者配置消息
  */
trait KafkaConsumerConfig extends Properties {

  import KafkaConsumerConfig._

  private val consumerPrefixWithDot = consumerPrefix + "."

  val allKeys = Seq(groupId,
    zookeeperConnect,
    zookeeperConnectionTimeOut,
    zookeeperSessionTimeOut,
    reBalanceBackOff,
    reBalanceMaxRetries,
    keyDeserializer,
    valueDeserializer,
    servers
  )

  lazy val conf = ConfigFactory.load()

  allKeys.map { key ⇒
    if (conf.hasPath(key)) {
      put(key.replace(consumerPrefixWithDot, ""), conf.getString(key))
    }
  }

}


object KafkaConsumerConfig {

  val consumerPrefix = "consumer"
  //Consumer Keys
  val groupId = s"$consumerPrefix.group.id"
  val zookeeperConnect = s"$consumerPrefix.zookeeper.connect"
  val topic = s"$consumerPrefix.topic"
  val zookeeperSessionTimeOut = s"$consumerPrefix.zookeeper.session.timeout.ms"
  val zookeeperConnectionTimeOut = s"$consumerPrefix.zookeeper.connection.timeout.ms"
  val reBalanceBackOff = s"$consumerPrefix.rebalance.backoff.ms"
  val reBalanceMaxRetries = s"$consumerPrefix.rebalance.max.retries"
  val keyDeserializer = s"$consumerPrefix.key.com.linewell.akkakafka.common.deserializer"
  val valueDeserializer = s"$consumerPrefix.value.com.linewell.akkakafka.common.deserializer"
  val servers = s"$consumerPrefix.bootstrap.servers"
  def apply(): KafkaConsumerConfig = new KafkaConsumerConfig {}
}