package org.apache.kafka.cep

import java.util.concurrent.TimeUnit

import org.apache.kafka.cep.utils.Observed

abstract class ChainDetector(timeFrame: Long, unit: TimeUnit) (implicit system:CEP)
extends Detector(timeFrame, unit) {

  override def handle(observed: Observed, event: Any) = {
    // TODO detects events in sequence - e.g. each consequent event is checked only if the previous one happened
  }

}
