package org.apache.kafka.cep

import java.util.concurrent.TimeUnit

import org.apache.kafka.cep.utils.Observed

class ValueAggregateDetector(sumDetectors: Detector*)(implicit system: CEP) extends Detector(0, TimeUnit.MINUTES) {
  override def toString: String = name + " { " + compositeDetector.toString + " }"

  val compositeDetector: Detector = new Composite(10, TimeUnit.MINUTES, sumDetectors)
  compositeDetector.addObserver(this)

  override def outstanding: List[Event] = compositeDetector.outstanding ++ super.outstanding

  override def handle(observed: Observed, event: Any) = {
    observed match {
      case `compositeDetector` ⇒ {
        val e = event.asInstanceOf[Event]
        if (e.isComplete) {
            update(e, sumDetectors.foldLeft(0.0)((a, b) ⇒ a + b.doubleValue(e)))
            notifyObservers(e)
        }
      }
      case _ ⇒ {}
    }
  }
}
