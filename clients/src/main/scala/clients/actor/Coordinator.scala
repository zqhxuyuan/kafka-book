package clients.actor

import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.stream.ActorMaterializer
import clients.actor.Command._
import com.typesafe.config.ConfigFactory

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

/**
  * Created by ctao on 16-1-23.
  * 调度类
  */
class Coordinator extends Actor with ActorLogging {

  import Coordinator._

  val conf = ConfigFactory.load()

  /**
    * 调度时间，类似定时器
    */
  val schedulerTime = conf.getInt("common.scheduler.time")

  /**
    * 写actor队列
    */
  val writerBuffer: ArrayBuffer[Option[ActorRef]] = new ArrayBuffer[Option[ActorRef]]()

  /**
    * 读actor队列
    */
  val readerBuffer: ArrayBuffer[Option[ActorRef]] = new ArrayBuffer[Option[ActorRef]]()

  /**
    * 物化
    */
  lazy val mat = ActorMaterializer()(context)

  override def receive: Receive = {
    /**
      * 如果是包含startnum和模式的消息，则调用构建方法
      */
    case msg@InitialMessage(StartNumber(num), mode) ⇒
      log.debug(s"Starting the numbers coordinator with $msg")
      buildWriteOrBuildRead(mode, num)

    /**
      * 无法识别的消息类型
      */
    case msg: InitialMessage ⇒
      log.error(s"Did not understand $msg")
      log.error("shutdown")
      context.system.shutdown()

    /**
      * actor在初始化结束后会向父节点发送消息，接收到读初始化结束后，启动一个调度时间的调度器，将stopactor
      * 的消息发给自己
      */
    case ReadInitialized(actorRef) ⇒
      log.debug(s"Reader initialized :${actorRef.path.toString}")
      context.system.scheduler.scheduleOnce(schedulerTime.seconds, self, Stop(actorRef))
      log.debug(s"end scheduler stop ${actorRef.path.toString}")

    /**
      * actor在初始化结束后会向父节点发送消息，接收到写初始化结束后，启动一个调度时间的调度器，将stopactor
      * 的消息发给自己
      */
    case WriteInitialized(actorRef) ⇒
      log.debug(s"Writer initialized:${actorRef.path.toString}")
      context.system.scheduler.scheduleOnce(schedulerTime.seconds, self, Stop(actorRef))
      log.debug(s"end scheduler stop ${actorRef.path.toString}")

    /**
      * 收到自己发送的stop消息后则对应队列移除actor
      */
    case Stop(actorRef) ⇒
      log.debug("Stopping the coordinator")

      writerBuffer -= Some(actorRef)
      readerBuffer -= Some(actorRef)

      log.debug(s"writeBuffer.length ${writerBuffer.length} and readerBuffer.length ${readerBuffer.length}")

      /**
        * 如果读写队列都为空，则给自己发送system的停止消息
        */
      if (writerBuffer.isEmpty && readerBuffer.isEmpty) {
        context.system.scheduler.scheduleOnce(1.seconds, self, Shutdown)
      }

    /**
      * 停止system
      */
    case Shutdown ⇒
      log.debug("Shutting down the app")
      context.system.shutdown()
      log.info("shutdown the app")
  }


  /**
    * 构建读写模式方法
 *
    * @param mode 模式
    * @param numActor actor数量
    */
  def buildWriteOrBuildRead(mode: Mode, numActor: Int): Unit = mode match {
    /**
      * 写模式，则写队列增减对应数量actor
      */
    case Mode.Write ⇒
      log.debug("write mode")
      (1 to numActor).foreach { x ⇒
        val writer = Some(context.actorOf(NumberProducer.props, name = s"writerActor-$x"))
        writerBuffer += writer
      }

    /**
      * 读模式，则读队列增减对应数量actor
      */
    case Mode.Read ⇒
      log.debug("read mode")
      (1 to numActor).foreach { x ⇒
        val reader = Some(context.actorOf(NumberConsumerByKafkaStream.props, name = s"readerActor-$x"))
        readerBuffer += reader
      }

    /**
      * 读写模式，则读写队列各增减对应数量actor
      */
    case Mode.Readwrite ⇒
      log.debug("readwrite mode")
      (1 to numActor).foreach { x ⇒
        val writer = Some(context.actorOf(NumberProducer.props, name = s"writerActor-$x"))
        val reader = Some(context.actorOf(NumberConsumerByKafkaStream.props, name = s"readerActor-$x"))
        writerBuffer += writer
        readerBuffer += reader
      }
  }
}


case object Coordinator {

  /**
    * 初始化消息
 *
    * @param name 消息类型
    * @param mode 模式
    */
  case class InitialMessage(name: Command, mode: Mode)

}