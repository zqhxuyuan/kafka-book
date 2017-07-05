package org.apache.kafka.cep.utils

import java.text.SimpleDateFormat

trait Observer {
  def handle(observed: Observed, impulse: Any)
}

trait Observed {

  val name: String = getClass.getSimpleName.replace("$", "")
  override def toString: String = name
  protected val dateFormatter = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss")

  private var observers: Set[Observer] = Set()
  def addObserver(observer: Observer) = observers += observer
  def removeObserver(observer: Observer) = observers -= observer
  def notifyObservers(event: Any) {
    for (observer ← observers) {
      observer.handle(this, event)
    }
  }
}