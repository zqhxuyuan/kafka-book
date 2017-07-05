package org.apache.kafka.cep

import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit._

import com.google.common.cache.{Cache, CacheBuilder, RemovalListener, RemovalNotification}
import org.apache.kafka.cep.utils.{Observed, Observer}

import scala.collection.JavaConverters._

abstract class Detector(val timeFrame: Long, val unit: TimeUnit = SECONDS)(implicit system: CEP)
  extends Observed with Observer {

  system.register(this)

  val timeFrameMillis = unit.toMillis(timeFrame)

  final def apply(observers: Observer*): Detector = {
    observers.map(o ⇒ addObserver(o))
    this
  }
  override def handle(observed: Observed, event: Any) = {
    //TODO probably reducer/proxy functionality will come here
  }

  /**
   * Future events are incomplete events that are
   * potentially going to happen in future or partial events which
   * are expected to have happened
   *
   * When future event in the cache is not removed by completion,
   * it eventually reaches it's lifetime and the Detector offers
   * the event is published to observers with incomplete status
   */
  val futureEvents: Cache[String, Event] = buildEventCache

  //TODO use ConcurrentSlidingWindow[Set[String]](timeFrame * 5, unit)
  val expiredEvents: Cache[String, java.lang.Long] = CacheBuilder.newBuilder.expireAfterWrite(timeFrame * 5, unit).build()
  //  protected def map(event: Event) = {}

  def doubleValue(event: Event): Double = doubleValue(event, "value")
  def doubleValue(event: Event, attr: String): Double = if (event.attributes.contains(name + "." + attr)) getAttribute(event, attr).toDouble else 0.0
  def update(event: Event, value: Any): Event = update(event, "value", value)
  def getAttribute(event: Event, attrName: String): String = event.attributes(name + "." + attrName)
  def update(event: Event, attrName: String, attrValue: Any): Event = {
    event.attributes += (name + "." + attrName) -> attrValue.toString
    event
  }

  /**
   * This method contains a critical section which use double-checking idiom
   * for high-concurrency and low synchronization footprint.
   */
  final def getFutureEvent(id: String, timestamp: Long, bits: Int): Option[Event] = {
    if (timeFrame == 0 || expiredEvents.getIfPresent(id) != null) {
      None
    } else {
      Some(futureEvents.getIfPresent(id) match {
        case existing: Event ⇒ existing
        case null ⇒ futureEvents.synchronized {
          futureEvents.getIfPresent(id) match {
            case existing: Event ⇒ existing
            case null ⇒ {
              val future = new Event(id, timestamp, bits)
              futureEvents.put(id, future)
              future
            }
          }
        }
      })
    }
  }

  final protected def mergeFutureEvent(constituent: Event, bit: Int, numBits: Int) = {
    getFutureEvent(constituent.id, constituent.timestamp, numBits) match {
      case None ⇒ {}
      case Some(future) ⇒ {
        if (!future.get(bit)) {
          future.set(bit, constituent.timestamp)
          future.attributes ++= constituent.attributes
        }
        if (future.isComplete) {
          futureEvents.invalidate(constituent.id)
        }
      }
    }
  }

  def outstanding: List[Event] = futureEvents.asMap.values.asScala.toList

  private def buildEventCache: Cache[String, Event] = CacheBuilder.newBuilder
    .expireAfterWrite(timeFrame * 5 / 4, unit)
    .removalListener(new RemovalListener[String, Event] {
      override def onRemoval(notification: RemovalNotification[String, Event]) = {
        val event = notification.getValue
        if (!event.isComplete) {
          expiredEvents.put(event.id, event.timestamp)
        }
        notifyObservers(event)
      }
    }).build.asInstanceOf[Cache[String, Event]]
}