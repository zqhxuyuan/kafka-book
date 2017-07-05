package org.apache.kafka.cep

import java.util.concurrent.atomic.AtomicInteger

import org.apache.kafka.cep.utils.{Observed, Observer, Shell, ShellCommand}

import scala.io.Source

class CEPShell(val system: CEP) extends Shell(system) {
  register(Exit)
  register(Dump)
  register(Next)
  register(Show)
}

object Dump extends ShellCommand[CEP] {
  override def getShortHelp = "show dump of the current outstanding composite events"
  override def invoke(stdin: Source, args: String) = {
    val dump = context.detectors.flatMap(d => d.outstanding)
    if (dump != null) {
      for (event <- dump) {
        println(event)
      }
    }
    println("---------------------------------------------------------------------------")
  }
}

object Exit extends ShellCommand[CEP] {
  override def getShortHelp = "Shutdown processors and exit."
  override def invoke(stdin: Source, args: String) = {
    println("Closing all connections..")
    //context.cassandra.close
    context.stop
    println("Bye.")
    shell.exit
  }
}

object Show extends ShellCommand[CEP] {
  override def getShortHelp = "Show regtistred detectors"
  override def invoke(stdin: Source, args: String) = {
    context.detectors.map(d => println(d))
  }
}
object Next extends ShellCommand[CEP] {
  val count: Int = 1
  override def getShortHelp = "[n]  Wait for next n top level events to occure [event-name-filter] Wait for the next single event of which name contains the filter argument"
  override def invoke(stdin: Source, args: String) = {
    var eventName: String = null
    var count = new AtomicInteger(1)
    args match {
      case null => {}
      case x if (x.forall(_.isDigit)) => count = new AtomicInteger(Integer.valueOf(args))
      case _ => { eventName = args }
    }

    val observer = new Observer {
      override def handle(observed: Observed, event: Any) = {
        if (count.decrementAndGet >= 0) {
          println(event)
        }
        context.synchronized {
          context.notifyAll
        }
      }
    }

    context.detectors.foreach(d => if (eventName == null || (d.name.toLowerCase contains eventName.toLowerCase)) {
      println("Adding observer for " + d);
      d.addObserver(observer);
    })

    try {
      while (count.get > 0) {
        context.synchronized {
          context.wait
        }
      }
    } finally {
      System.out.println("---------------------------------------------------------------------------")
      for (d <- context.detectors) yield {
        d.removeObserver(observer);
      }

    }
  }
}
