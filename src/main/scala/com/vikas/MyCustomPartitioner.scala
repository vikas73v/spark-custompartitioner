package com.vikas

import org.apache.spark.Partitioner

abstract class MyCustomPartitioner(parts: Int) extends Partitioner {
  override def numPartitions: Int = parts

  override def getPartition(key: Any): Int = {
    val out= toVik(key.toString)
    out
  }

  override def equals(other :Any) : Boolean = other match
    {
    case vik: MyCustomPartitioner =>
      vik.numPartitions == parts
    case _ => false
  }

  def toVik(s:String) : Int = s match
  {
    case s: String => {
      if (s(0).toUpper > 'J') 1 else 0

      }
  }
}