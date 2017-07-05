package org.apache.kafka.cep

import scala.collection.BitSet

final class Event(val id: String, var timestamp: Long, val fixedSize: Int) extends Comparable[Event] {

  def this(id: String, timestamp: Long) {
    this(id, timestamp, 0)
  }

  def this(id: String) {
    this(id, System.currentTimeMillis, 0)
  }

  override def hashCode: Int = id.hashCode
  override def equals(obj: Any) = obj.isInstanceOf[Event] && obj.asInstanceOf[Event].id.equals(id)

  val name: String = getClass.getSimpleName.replace("$", "")
  def ageInMillis: Long = System.currentTimeMillis - timestamp
  def ageInSeconds: Double = math.round(ageInMillis.toDouble / 10.0) / 100.0

  var bitset = BitSet()
  def cardinality: Int = bitset.size
  def isComplete: Boolean = cardinality == fixedSize
  def get(bit: Int) = bitset(bit)
  def set(bit: Int, timestamp: Long) {
    bitset += bit
    if (timestamp < this.timestamp) {
      this.timestamp = timestamp;
    }
  }

  private[cep] var attributes: Map[String, String] = Map()

  override def toString: String = {
    if (fixedSize == 0) {
      name + " " + id + " -> " + attributes.toString;
    } else {
      "(" + ageInSeconds + "s)" +
        " " + name + " " + id +
        " " + (if (isComplete) "√" else "≈") +
        " " + bitset.foldLeft("|")(_.toString + _.toString + "|") + " (" + cardinality + "/" + fixedSize + ")" +
        " -> " + attributes.toString
    }
  }

  /**
   * Need to implement scala.Ordered
   */
  override def compareTo(o: Event): Int = {
    ageInSeconds.intValue - o.ageInSeconds.intValue;
  }

}