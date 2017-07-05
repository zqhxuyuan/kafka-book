package org.apache.kafka.cep

import java.util.concurrent.TimeUnit

import org.apache.kafka.cep.utils.Observed

class ValueLogRatioDetector(timeFrameSec:Long, unit:TimeUnit, val d1: Detector, val d2: Detector, val threshold: Double)(implicit system: CEP)
extends Detector(0) {
  val compositeDetector = new Composite(timeFrameSec, unit, Seq(d1, d2))
  compositeDetector.addObserver(ValueLogRatioDetector.this)
  override def handle(observed: Observed, event: Any) = observed match {
    case `compositeDetector` ⇒ {
      val e = event.asInstanceOf[Event]
      if (e.isComplete) {
          val logRatio = math.log(d1.doubleValue(e) / d2.doubleValue(e))
          if (logRatio.abs > threshold) {
              update(e, logRatio)
              notifyObservers(e)
          }
      }
    }
    case _ ⇒ {}
  }
}