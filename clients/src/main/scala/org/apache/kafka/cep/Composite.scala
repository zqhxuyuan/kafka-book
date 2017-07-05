package org.apache.kafka.cep

import java.util.concurrent.TimeUnit

import org.apache.kafka.cep.utils.Observed

class Composite(timeFrame: Long, unit: TimeUnit, compositeDetectors: Seq[Detector])(implicit val system: CEP)
  extends Detector(timeFrame, unit) {

  def this(detector: Detector)(implicit system: CEP) = this(0, TimeUnit.SECONDS, Seq(detector))

  val detectors: Seq[Detector] = compositeDetectors.map(detector ⇒ detector(Composite.this))

  override def toString: String = name + " = {" + detectors.foldLeft("")(_ + "\n" + _).replace("\n", "\n\t") + "\n}"

  override def outstanding: List[Event] = detectors.flatMap(d ⇒ d.outstanding).toList ++ super.outstanding

  override def handle(observed: Observed, impulse: Any) = observed match {
    case detector: Detector if (detectors.contains(detector)) ⇒ {
      val event = impulse.asInstanceOf[Event]
      if (event.isComplete) {
        mergeFutureEvent(event, detectors.indexOf(detector), detectors.length)
      }
    }
    case _ ⇒ {}
  }

}
