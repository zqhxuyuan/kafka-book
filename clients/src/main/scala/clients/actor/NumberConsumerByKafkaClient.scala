package clients.actor

import java.util

import akka.actor.SupervisorStrategy.Escalate
import akka.actor._
import clients.actor.Command._
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConversions._

/**
  * Created by ctao on 16-1-27.
  * 用kafkaClinet实现的消费类
  */
class NumberConsumerByKafkaClient extends Actor with ActorLogging {

  private var consumer: KafkaConsumer[String, Long] = _
  private lazy val conf = ConfigFactory.load()
  private val topic = conf.getString("consumer.topic")
  private val timeOut = conf.getLong("common.timeout")

  /**
    * 启动前调用
    */
  override def preStart(): Unit = {
    initConsumer()
    context.parent ! ReadInitialized(self)
    self ! StartConsume
  }

  /**
    * 如果收到开始消费则进行消费动作
    */
  override def receive: Receive = {
    case StartConsume ⇒ consume(timeOut)
  }

  /**
    * 终止前调用
    */
  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    log.debug("stopping all consumer")
    consumer.unsubscribe()
    consumer.close()
    log.debug("stop all consumer")

  }

  /**
    * 消费方法
    */
  private def consume(timeOut: Long): Unit = {
    consumer.poll(timeOut).foreach { record ⇒
      log.info(s"${self.path.toString} receive ${record.key} value ${record.value} " +
        s"offset ${record.offset} partition ${record.partition} topic ${record.topic}")
    }
    consumer.commitAsync()
  }

  /**
    * actor的策略
    */
  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy() {
    case e: Exception ⇒
      log.error(s"Read failed $e")
      Escalate
  }

  /**
    * 初始化actor
    */
  private def initConsumer() = {
    log.debug(s"Config ${KafkaConsumerConfig()}")
    consumer = new KafkaConsumer[String, Long](KafkaConsumerConfig())
    consumer.subscribe(Vector(topic), new ConsumerRebalanceListener() {
      override def onPartitionsAssigned(collection: util.Collection[TopicPartition]): Unit = {
        println("assigned: " + collection.mkString(","))
      }

      override def onPartitionsRevoked(collection: util.Collection[TopicPartition]): Unit = {
        println("revoked: " + collection.mkString(","))
      }
    })
  }
}


object NumberConsumerByKafkaClient {
  def props: Props = Props(new NumberConsumerByKafkaClient)
}