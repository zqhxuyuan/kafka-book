package clients.actor

import akka.actor.ActorRef

sealed trait Mode

object Mode {

  /**
    * 读模式
    */
  case object Read extends Mode

  /**
    * 写模式
    */
  case object Write extends Mode

  /**
    * 混合模式
    */
  case object Readwrite extends Mode
}

sealed trait Command

object Command {

  /**
    * 读模式actor初始化结束
 *
    * @param actorRef 路径
    */
  case class ReadInitialized(actorRef: ActorRef) extends Command

  /**
    * 写模式actor初始化结束
 *
    * @param actorRef 路径
    */
  case class WriteInitialized(actorRef: ActorRef) extends Command

  /**
    * 关闭actor
 *
    * @param actorRef 路径
    */
  case class Stop(actorRef: ActorRef) extends Command

  /**
    * 停止系统
    */
  case object Shutdown extends Command

  /**
    * 开启任务
 *
    * @param num 初始actor数量
    */
  case class StartNumber(num: Int) extends Command

  /**
    * 开启消费
    */
  case object StartConsume extends Command

  /**
    * 开启生产
    */
  case object StartProduce extends Command

}