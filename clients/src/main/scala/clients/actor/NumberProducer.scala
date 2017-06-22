package clients.actor

import akka.actor.SupervisorStrategy.Resume
import akka.actor._
import akka.event.LoggingReceive
import clients.actor.Command._
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer._


/**
  * Created by ctao on 16-1-23.
  */
class NumberProducer extends Actor with ActorLogging {
  private val conf = ConfigFactory.load()
  /**
    * 产生数字数量
    */
  val numMessage = conf.getInt("common.numMessage")

  val topic = conf.getString("consumer.topic")


  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy() {
    case e: Exception ⇒
      //can handle failing here
      log.error(s"Write failed $e")
      Resume
  }

  private var producer: KafkaProducer[String, Long] = _

  /**
    * 启动前执行
    */
  override def preStart(): Unit = {
    producer = initProducer()
    context.parent ! WriteInitialized(self)
    self ! StartProduce
  }

  /**
    * 接收到开始生产后则调用生产函数
    */
  override def receive: Receive = LoggingReceive {
    case StartProduce ⇒ produce(producer, numMessage)
  }

  /**
    * 结束前调用
    */
  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    log.debug("closing producer")
    producer.close()
    log.debug("closed producer")
  }

  /**
    * 生产函数
    * @param producer 生产者
    * @param numMessage 消息数量
    */
  private def produce(producer: KafkaProducer[String, Long], numMessage: Int): Unit = {
    (1 to numMessage).foreach { messageNum ⇒
      val message = new ProducerRecord[String, Long](topic, (messageNum + 1).toString, messageNum)
      producer.send(message, new Callback {
        override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
          val maybeMetadata = Option(metadata)
          val maybeException = Option(exception)
          if (maybeMetadata.isDefined) {
            log.info(s"actor ${self.path.toString}: $messageNum onCompletion offset ${metadata.offset},partition ${metadata.partition}")
          }
          if (maybeException.isDefined) {
            log.error(exception, s"$messageNum onCompletion received error")
          }
        }
      })
    }
  }

  /**
    * 初始化
    * @return 生产者
    */
  private def initProducer(): KafkaProducer[String, Long] = {
    log.debug(s"Config ${KafkaProducerConfig()}")
    new KafkaProducer[String, Long](KafkaProducerConfig())
  }
}


object NumberProducer {
  def props: Props = Props(new NumberProducer)
}