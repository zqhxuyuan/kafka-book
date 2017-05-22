package base.data

import scala.collection.mutable

case class Operation[A](partitions: List[A], name: String) {
  override def toString = name

  def tryComplete(): Unit ={
   println("trying complete me,but may not completed")
  }
}

object CrossReference {

  def basicCross(): Unit ={
    val operation1: List[Int] = List(1,2,3,4,5)
    val operation2: List[Int] = List(1,3,5,6,7)

    //demo list cross reference
    val watchers = mutable.Map[Int, List[Int]]()

    for(key <- operation1) {
      watchers += key -> operation1
    }
    //注意Map会覆盖,而不是追加! 所以不符合!
    for(key <- operation2) {
      watchers += key -> operation2
    }

    for((k,v) <- watchers) {
      val key: Int = k
      val opeartion: List[Int] = v
      println(key + "==>" + opeartion.mkString(","))
    }
    println("------------------------")
  }

  def main(args: Array[String]) {
    val operation1: List[Int] = List(1,2,3,4,5)
    val operation2: List[Int] = List(1,3,5,6,7)

    //demo kafka purgatory data structure
    val purgatory = new Purgatory[Int]
    purgatory.watch(Operation(operation1, "operation1"))
    purgatory.watch(Operation(operation2, "operation2"))
    purgatory.print()

    //purgatory.removeKey(1)
    //purgatory.print()

    purgatory.remove(Operation(operation1, "operation1"))
    purgatory.print()
  }
}

class Purgatory[A] {

  val watchers = mutable.Map[A, List[Operation[A]]]()

  def watch(delayedOperation: Operation[A]): Unit ={
    for(partition <- delayedOperation.partitions){
      val listOpt = watchers.get(partition)
      val opers = listOpt match {
        case Some(list) => delayedOperation :: list
        case None => List(delayedOperation)
      }
      watchers += partition -> opers
    }
  }

  def removeKey(key: A): Unit ={
    watchers.remove(key)
  }
  //删除Watcher集合中, 包含指定Operation的所有元素
  def remove(operation: Operation[A]) = {
    for(ele <- watchers) {
      val list = ele._2
      if(list.contains(operation)) {
        val newList = (list.toBuffer - operation).toList
        if(newList.size == 0){
          watchers.remove(ele._1)
        }else{
          watchers += ele._1 -> newList
        }
      }
    }
  }

  def checkAndComplete(key: A): Unit ={
    val listOpt = watchers.get(key)
    listOpt match {
      case Some(list) => for(oper <- list) oper.tryComplete()
      case None =>
    }
  }

  def print(): Unit ={
    println("----------------------------")
    for((k,v) <- watchers) {
      println(k + "==>" + v.mkString(","))
    }
  }
}
