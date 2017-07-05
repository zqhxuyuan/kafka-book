package org.apache.kafka.cep

import java.util.concurrent.TimeUnit

import org.apache.kafka.cep.utils.{ConcurrentSlidingWindow, Observed}

class ValueStDevDetector(slidingWindow: Int, unit: TimeUnit, val threshold: Double, underlyingDetector: Detector)(implicit system:CEP)
  extends Detector(underlyingDetector.timeFrame, underlyingDetector.unit) {

  override def toString = super.toString + " { " + underlyingDetector.toString + " }"
  override def outstanding: List[Event] = super.outstanding ++ underlyingDetector.outstanding
  underlyingDetector.addObserver(this)

  private val window = new ConcurrentSlidingWindow[Double](slidingWindow, unit)

  override def handle(observed: Observed, event: Any) = observed match {
    case `underlyingDetector` ⇒ {
      val e = event.asInstanceOf[Event]
      if (e.isComplete) {
          val value = underlyingDetector.doubleValue(e)
          window.add(e.timestamp, value)
          update(e, "point", value)
          val dist = window.dist
          if (!dist.mean.isNaN && dist.stdev > 0 && dist.n >2) {
            val dev: Double = math.abs(value - dist.mean) / dist.stdev
            if (dev > threshold) {
                update(e, "mean", dist.mean)
                update(e, "stdev", dist.stdev)
                update(e, dev)
                notifyObservers(e)
            }
          }
      }
    }
    case _ ⇒ {}
  }
}
