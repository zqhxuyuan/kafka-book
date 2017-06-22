package clients.actor

import java.util.concurrent.Executors

import akka.actor.SupervisorStrategy.Escalate
import akka.actor._
import clients.actor.Command._
import clients.actor.NumberConsumerByKafkaStream.Consume
import com.typesafe.config.ConfigFactory
import kafka.consumer.{Consumer, ConsumerConfig, ConsumerConnector, KafkaStream}
import kafka.message.MessageAndMetadata
import org.apache.kafka.common.serialization.StringDeserializer
import scala.async.Async._
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import scala.util.{Failure, Success, Try}
import kafka.serializer._

/**
  * Created by ctao on 16-1-23.
  * kafkastream实现的消费，含有阻塞
  */
class NumberConsumerByKafkaStream extends Actor with ActorLogging {

  val conf = ConfigFactory.load()
  /**
    * 消费连接
    */
  private var consumer: Try[ConsumerConnector] = _
  /**
    * 线程池线程数，主要提供给kafka消费，将阻塞放在单独池中
    */
  val threadNum = conf.getInt("common.threadNum")
  private val executor = Executors.newFixedThreadPool(threadNum)
  val topic = conf.getString("consumer.topic")
  /**
    * kafkaStream，在关闭前clean
    */
  private var streams: Option[List[KafkaStream[String, Long]]] = None

  override def receive: Receive = {
    /**
      * 开始消费，则获取stream传递给自己
      */
    case StartConsume ⇒ consumer.foreach { (consumer: ConsumerConnector) =>
      //      val consumerStreams = consumer.createMessageStreams(Map(topic -> 1), Decoder(topic, new StringDeserializer), Decoder(topic, new LongDeserializer))
      //      val consumerStreams = consumer.createMessageStreams[String, Long](Map(topic -> 1))
      val consumerStreams = consumer.createMessageStreams(Map(topic -> 1), new StringDecoder(), new LongDecoder())

      streams = Option(consumerStreams(topic))
      if (streams.isDefined) {
        log.info(s"Got streams ${streams.get.length} $streams")
        streams.get.foreach { kafkaStream ⇒
          self ! Consume(kafkaStream)
        }
      }
    }

    /**
      * 如果是stream，则放在池中
      */
    case Consume(kafkaStream) ⇒
      log.info(s"Handling KafkaStream ${kafkaStream.clientId}")
      implicit val executionContextExecutor: ExecutionContextExecutor = ExecutionContext.fromExecutor(executor)
      async {
        kafkaStream.iterator().foreach {
          case msg: MessageAndMetadata[String, Long] ⇒
            log.info(s"${self.path.toString} : kafkaStream ${kafkaStream.clientId} " +
              s" received offset ${msg.offset}  partition ${msg.partition} value ${msg.message}")
        }
      }
  }


  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy() {
    case e: Exception ⇒
      log.error(s"Read failed $e")
      Escalate
  }

  /**
    * 开始前动作
    */
  override def preStart(): Unit = {
    super.preStart()
    consumer = Try(Consumer.create(consumerConfig))
    consumer match {
      case Success(c) ⇒ context.parent ! ReadInitialized(self)
        self ! StartConsume
      case Failure(e) ⇒
        log.error(e, "Could not create kafkaConsumer")
        context.parent ! Shutdown
    }
  }

  /**
    * 结束前动作
    */
  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    if (streams.isDefined) {
      log.debug("cleaning streams")
      streams.get.foreach(_.clear())
      log.debug("cleaned streams")
    }
    log.debug("stopping all consumer")
    consumer.foreach(_.shutdown())
    log.debug("stop all consumer")
    log.debug("shutting down execution")
    executor.shutdown()
    log.debug("shutdown execution")
  }

  /**
    * 消费配置
    */
  private val consumerConfig: ConsumerConfig = new ConsumerConfig(KafkaConsumerConfig())

}


object NumberConsumerByKafkaStream {

  /**
    * 返回包含消费者的参数
    */
  def props: Props = Props(new NumberConsumerByKafkaStream())

  /**
    * 包含kafkaStream的消费类
    */
  private case class Consume(kafkaStream: KafkaStream[String, Long])

}