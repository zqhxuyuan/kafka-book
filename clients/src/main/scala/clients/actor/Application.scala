package clients.actor

import akka.actor.{ActorSystem, Props}
import clients.actor.Command._
import clients.actor.Coordinator._
import clients.actor.Mode._
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory

/**
  * Created by ctao on 16-1-23.
  */
object Application extends App {

  val log = LoggerFactory.getLogger(this.getClass)

  val system = ActorSystem("Kafka")

  /**
    * 调度actor
    */
  val coordinator = system.actorOf(Props(new Coordinator), name = "coordinator")

  val conf = ConfigFactory.load()
  private val numActor = conf.getInt("common.actor")
  log.info(s"start app")

  /**
    * app的模式
    */
  val appMode = conf.getString("common.mode") match {
    case s: String ⇒ s.toUpperCase match {
      case "READ" ⇒ Read
      case "WRITE" ⇒ Write
      case "READWRITE" ⇒ Readwrite
    }
    case _ ⇒ throw  new IllegalArgumentException("can't load mode")
  }

  /**
    * 启动调度
    */
  coordinator ! InitialMessage(StartNumber(numActor),appMode)

}