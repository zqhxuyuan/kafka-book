package org.apache.kafka.cep

import java.util.concurrent.TimeUnit

import org.apache.kafka.cep.utils.{ConcurrentSlidingWindow, Observed}

class SequenceDetector(timeFrame: Long, unit: TimeUnit, val limit: Int, val underlyingDetector: Detector)(implicit system: CEP)
  extends Detector(timeFrame, unit) {

  override def toString = super.toString + " { " + underlyingDetector.toString + " }"
  override def outstanding: List[Event] = super.outstanding ++ underlyingDetector.outstanding
  underlyingDetector.addObserver(this)

  val window = new ConcurrentSlidingWindow[Seq[Event]](timeFrame, unit)

  override def handle(observed: Observed, event: Any) = observed match {
    case `underlyingDetector` ⇒ {
      val e = event.asInstanceOf[Event]
      if (e.isComplete) {
          window.add(e.timestamp, Seq(e))
          val sequence = window.sum
          if(sequence.size >= limit) {
            sequence.map(event => notifyObservers(update(event,sequence.size)))
          }
      }
    }
    case _ => {}
  }
}