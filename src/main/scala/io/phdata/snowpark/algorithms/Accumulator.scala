package io.phdata.snowpark.algorithms

import scala.collection.mutable

class Accumulator[T] {
  var listAccu: List[List[T]] = Nil
  var mapAccu: mutable.Map[List[T], List[Int]] = new mutable.HashMap[List[T], List[Int]]()
  var counter = 0
  def clear(): Unit = {
    listAccu = Nil
    mapAccu.clear()
    counter = 0
  }
  def push(listElem: List[T]): Unit = {
    listAccu = listElem :: listAccu
    val mapElem = mapAccu.getOrElse(listElem, Nil)
    mapAccu.put(listElem, counter :: mapElem)
    counter += 1
  }
  def getSize: Int = counter
  def getSizeDistinct: Int = mapAccu.size
  def getSize(listElem: List[T]): Int = mapAccu.getOrElse(listElem, Nil).length
  def getIndexFirst(listElem: List[T]): Int = mapAccu.getOrElse(listElem, List(-1)).head
}
