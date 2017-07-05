package org.apache.kafka.cep

import org.apache.kafka.cep.utils.Observed

class ExpiredEventDetector(val underlyingDetector: Detector)(implicit system: CEP)
  extends Detector(1) {
  underlyingDetector.addObserver(ExpiredEventDetector.this)
  override def handle(observed: Observed, event: Any) = observed match {
    case `underlyingDetector` if (!event.asInstanceOf[Event].isComplete) ⇒ {
      val underlyingEvent = event.asInstanceOf[Event]
      getFutureEvent(underlyingEvent.id, underlyingEvent.timestamp, 1) match {
        case None ⇒ {}
        case Some(future) ⇒ {
          update(future, underlyingEvent.ageInMillis)
          mergeFutureEvent(underlyingEvent, 0, 1)
        }
      }
    }
    case _ ⇒ {}
  }
}