package org.apache.kafka.cep.utils

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{ConcurrentNavigableMap, ConcurrentSkipListMap, TimeUnit}

import org.apache.kafka.cep.Event

trait Mergable[X] {
  def init: X
  def merge(arg1: X, arg2: X): X
}

trait Aggregate[X] {
  def aggregate(args: ConcurrentNavigableMap[Long,X]): Moments
}

object Mergable {
  implicit val DoubleIsMergable = new Mergable[Double] {
    override def init = 0.0
    override def merge(arg1: Double, arg2: Double) = arg1 + arg2 
  }
  implicit val IntIsMergable = new Mergable[Int] {
    override def init = 0
    override def merge(arg1: Int, arg2: Int) = arg1 + arg2
  }
  implicit val EventsAreMergable = new Mergable[Seq[Event]] {
    override def init = Seq[Event]()
    override def merge(arg1: Seq[Event], arg2: Seq[Event]) = arg1 ++ arg2 
  }
}

object Aggregate {
  implicit val DoublesCanBeAggregated = new Aggregate[Double] {
    def aggregate(args: ConcurrentNavigableMap[Long,Double]): Moments = aggregateDouble(args)
  }
  def aggregateDouble(args: ConcurrentNavigableMap[Long,Double]): Moments = {
    {
      var n: Long = 0
      var sum: Double = 0
      var sum2: Double = 0
      var cursor: Long = 0
      var overflow: Double = 0
      val i = args.entrySet.iterator
      while(i.hasNext) {
        val kv = i.next
        val t = kv.getKey
        val value: Double = kv.getValue
        val k = t - cursor
        if (cursor == 0) {
          n = 1
          overflow = value
          sum = value
          sum2 = math.pow(value, 2)
        } else if (overflow > 0) {
          n = n + k
          sum = (value + overflow) / 2 * (k + 1)
          sum2 = math.pow(value, 2) * (k / 2 + 1) + math.pow(overflow, 2) * (k / 2 + 1)
          overflow = 0
        } else {
          n = n + k
          sum = sum + value * k
          sum2 = sum2 + math.pow(value, 2) * k
        }
        cursor = t
      }
      Moments(n, sum, sum2)
    }
  }
}

case class Moments(val n: Double, val nx: Double, val nxsqr: Double) extends Serializable {
  def mean = nx / n
  def stdev = math.sqrt(nxsqr / n - math.pow(nx / n, 2))
}

class ConcurrentSlidingWindow[T](val window: (Long, TimeUnit)) {

  val milliseconds: Long = window._2.toMillis(window._1)
  val unit: Long = window._2.toMillis(1)
  val precision: Long = 1000
  private val measurements = new ConcurrentSkipListMap[Long, T]
  private val maxTimestamp = new AtomicLong(0)

  final def add(timestamp: Long, value: T)(implicit f: Mergable[T]) {
    val currentMaxTimestamp = maxTimestamp.get
    if (timestamp >= currentMaxTimestamp - milliseconds) {
      if (currentMaxTimestamp < timestamp) {
        maxTimestamp.compareAndSet(currentMaxTimestamp, timestamp)
      }
      update(timestamp, value)
    }
  }

  final protected def update(timestamp: Long, value: T)(implicit f: Mergable[T]) {
    val key = timestamp / precision
    @volatile var existing: Option[T] = Option(measurements.putIfAbsent(key, value))
    while (existing != None) {
      if (measurements.replace(key, existing.get, f.merge(value, existing.get))) {
        existing = None
      } else {
        existing = Option(measurements.get(key))
      }
    }
  }

  final def data:ConcurrentNavigableMap[Long,T] = data(maxTimestamp.get)
  final def data(ceiling: Long):ConcurrentNavigableMap[Long,T] = {
    val lastUnit = math.floor( ceiling.toDouble / precision).toLong - 1
    val firstUnit = lastUnit - (milliseconds.toDouble / precision).toLong + 1
    measurements.headMap(firstUnit.toLong).clear
    measurements.headMap(lastUnit, true)
  }

  final def sum(implicit f: Mergable[T]): T = sum(maxTimestamp.get)

  final def sum(ceiling: Long)(implicit f: Mergable[T]): T = {
    var result: T = f.init
    val i = data(ceiling).entrySet.iterator
    while (i.hasNext) {
      val nextEntry = i.next
      if (nextEntry.getKey <= ceiling) {
          val value = nextEntry.getValue
          result = if (result == None) value else f.merge(result, value)
      }
    }
    result
  }

  final def dist(implicit f: Aggregate[T]): Moments = dist(maxTimestamp.get)

  final def dist(ceiling: Long)(implicit f: Aggregate[T]): Moments = {
    f.aggregate(data(ceiling))
  }

}