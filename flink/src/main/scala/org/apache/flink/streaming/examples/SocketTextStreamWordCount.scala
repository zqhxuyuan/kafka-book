package org.apache.flink.streaming.examples

import org.apache.flink.streaming.api.scala._

/**
  * This example shows an implementation of WordCount with data from a text socket.
  * To run the example make sure that the service providing the text data is already up and running.
  *
  * To start an example socket text stream on your local machine run netcat from a command line,
  * where the parameter specifies the port number:
  *
  * {{{
  *   nc -lk 9999
  * }}}
  *
  * Usage:
  * {{{
  *   SocketTextStreamWordCount <hostname> <port>
  * }}}
  *
  * This example shows how to:
  *
  *   - use StreamExecutionEnvironment.socketTextStream
  *   - write a simple Flink Streaming program in scala
  *   - write and use user-defined functions
  */
object SocketTextStreamWordCount {

  def main(args: Array[String]) {
    val hostName = "localhost"
    val port = 9999

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // create streams for names and ages by mapping the inputs to the corresponding objects
    val text = env.socketTextStream(hostName, port)
    val counts = text.flatMap { _.toLowerCase.split("\\W+") filter { _.nonEmpty } }
      .map { (_, 1) }
      .keyBy(0)
      .sum(1)

    counts print

    println(env.getExecutionPlan)
    //env.execute("Scala WordCount from SocketTextStream Example")
  }
}