package clients.akka.streams.intro

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.stream.{KillSwitches, UniqueKillSwitch, OverflowStrategy, ActorMaterializer}
import akka.stream.scaladsl._

import scala.concurrent.Future
import scala.util.{Failure, Success}
import scala.concurrent._
import ExecutionContext.Implicits.global

/**
  * Created by zhengqh on 17/6/26.
  *
  * https://tech.zalando.com/blog/about-akka-streams/?gh_src=4n3gxh1
  * https://opencredo.com/introduction-to-akka-streams-getting-started/
  */
object MyFirstStream {
  // Akka Streams based on top of the Akka actor system
  implicit val system = ActorSystem("MyActorSystem")

  // Materializer: actually runs streams, allocating all resources that are necessary and starting all the mechanics.
  // It executes stream stages on top of Akka actors
  implicit val materializer = ActorMaterializer()


  def fromZero(): Unit = {
    // RunnableGraph: blueprints are immutable
    val helloWorldStream: RunnableGraph[NotUsed] =
      Source.single("Hello world")
        .via(Flow[String].map(s => s.toUpperCase()))
        .to(Sink.foreach(println))

    helloWorldStream.run() // materialize the stream

    val materializedValue: NotUsed = helloWorldStream.run()

    // syntactic sugar
    val helloWorldStream2: RunnableGraph[NotUsed] =
      Source.single("Hello world")
        .map(s => s.toUpperCase())
        .to(Sink.foreach(println))
    helloWorldStream2.run() // materialize the stream

    // Future value
    val helloWorldStream3: RunnableGraph[Future[Done]] =
      Source.single("Hello world")
        .map(s => s.toUpperCase())
        .toMat(Sink.foreach(println))(Keep.right)

    val doneFuture: Future[Done] = helloWorldStream3.run()

    // Materialized values can be consider as of some kind of external handler to a materialized stream.
    // That's why we can defile onComplete callback below
    doneFuture.onComplete {
      case Success(Done) =>
        println("Stream finished successfully.")
      case Failure(e) =>
        println(s"Stream failed with $e")
    }

    // Kill Switch
    val helloWorldStream4: RunnableGraph[(UniqueKillSwitch, Future[Done])] =
      Source.single("Hello world")
        .map(s => s.toUpperCase())
        .viaMat(KillSwitches.single)(Keep.right)   // We use Keep.right to preserve it and pass downstream,
        .toMat(Sink.foreach(println))(Keep.both)   // use Keep.both to preserve both KillSwitch and Future[Done].
    // As materialized view returned both kill-switch and future, so run method return this two exactly types preserved
    val (killSwitch, _): (UniqueKillSwitch, Future[Done]) = helloWorldStream4.run()
    killSwitch.shutdown()
    killSwitch.abort(new Exception("Exception from KillSwitch"))
  }

  def main(args: Array[String]): Unit = {
    // Define Source
    val sourceFromRange = Source(1 to 10)
    val sourceFromIterable = Source(List(1,2,3))
    val sourceFromFuture = Source.fromFuture(Future.successful("hello"))
    val sourceWithSingleElement = Source.single("just one")
    val sourceEmittingTheSameElement = Source.repeat("again and again")
    val emptySource = Source.empty

    // Define Sink
    val sinkPrintingOutElements = Sink.foreach[String](println(_))
    val sinkCalculatingASumOfElements = Sink.fold[Int, Int](0)(_ + _)
    val sinkReturningTheFirstElement = Sink.head
    val sinkNoop = Sink.ignore

    // Define Flows
    val flowDoublingElements = Flow[Int].map(_ * 2)
    val flowFilteringOutOddElements = Flow[Int].filter(_ % 2 == 0)
    val flowBatchingElements = Flow[Int].grouped(10)
    val flowBufferingElements = Flow[String].buffer(1000, OverflowStrategy.backpressure)

    val streamCalculatingSumOfElements: RunnableGraph[Future[Int]] =
      sourceFromIterable.toMat(sinkCalculatingASumOfElements)(Keep.right)
    val streamCalculatingSumOfDoubledElements: RunnableGraph[Future[Int]] =
      sourceFromIterable.via(flowDoublingElements).toMat(sinkCalculatingASumOfElements)(Keep.right)

    // Run Akka Stream
    val sumOfElements: Future[Int] = streamCalculatingSumOfElements.run()
    sumOfElements.foreach(println) // we expect to see 6
    val sumOfDoubledElements: Future[Int] = streamCalculatingSumOfDoubledElements.run()
    sumOfDoubledElements.foreach(println) // we expect to see 12

    // runs the stream by attaching specified sink
    sourceFromIterable.
      via(flowDoublingElements).
      runWith(sinkCalculatingASumOfElements).
      foreach(println)

    // runs the stream by attaching sink that folds over elements on a stream
    Source(List(1,2,3)).
      map(_ * 2).
      runFold(0)(_ + _).
      foreach(println)
  }

}
