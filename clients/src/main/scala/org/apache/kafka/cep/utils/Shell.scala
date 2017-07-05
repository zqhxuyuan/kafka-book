package org.apache.kafka.cep.utils

import java.io.IOException

import scala.io._

class Shell[T](context: T) extends Runnable {

  val consoleName: String = context.getClass.getSimpleName.replace("$", "")

  var commands = Map[String, ShellCommand[T]]()

  def register(command: ShellCommand[T]): Shell[T] = {
    commands += command.getClass.getSimpleName.replace("$","").toLowerCase -> command
    command.registerContext(this, context)
    this
  }

  register(new Help[T])

  val thread = new Thread(this)
  var exitting = false

  def init = {
    thread.start
    try {
      thread.join
    } catch {
      case e: InterruptedException => System.exit(1)
    }
    System.exit(0)
  }

  override def run() = {
    print("\n" + consoleName + ">")
    try {
      for (ln <- Source.stdin.getLines) {
        val cmd = ln.split(" ")
        var args: String = null
        var command: ShellCommand[T] = commands("help")
        if (cmd(0) != null && !cmd(0).isEmpty) {
          if (!commands.contains(cmd(0))) {
            println("Unknown command " + cmd(0));
          } else {
            args = if (cmd.length == 2) cmd(1) else null
            command = commands(cmd(0))
          }
          try {
            command invoke (Source.stdin, args)
          } catch {
            case e: Throwable => e.printStackTrace();
          } finally {
            if (!exitting) {
              print("\n" + consoleName + ">")
            }
          }
        }
      }
    } catch {
      case e: IOException => exit
    }
  }

  def exit = {
    exitting = true;
    Source.stdin.close
  }

}

trait ShellCommand[T] {

  def getShortHelp: String;

  def printDetailedHelp = println(getShortHelp)

  var shell: Shell[T] = null

  var context: T = _

  def registerContext(shellInstance: Shell[T], contextInstance: T) = {
    this.shell = shellInstance
    this.context = contextInstance
  }

  @throws(classOf[IOException])
  def invoke(stdin: Source, args: String)

}


class Help[S] extends ShellCommand[S] {
  override def getShortHelp = "Show help"
  override def printDetailedHelp = println("help [command] Shows help for individual command")
  override def invoke(stdin: Source, args: String) = {
    args match {
      case x if (x != null && shell.commands.contains(args)) => {
        shell.commands(args) printDetailedHelp
      }
      case _ => {
        println("\nAvailable commands:\n")
        shell.commands.map(x => {
          print(x._1 + "                                                      " slice (0, 20))
          println(x._2.getShortHelp)
        })
      }
    }
  }
}