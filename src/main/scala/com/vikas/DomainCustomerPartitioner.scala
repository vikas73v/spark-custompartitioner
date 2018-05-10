package com.vikas

import org.apache.spark.Partitioner

abstract class DomainCustomerPartitioner(numParts: Int) extends Partitioner {

  override def numPartitions: Int = numParts

  override def getPartition(key: Any): Int = {
    val domain = new java.net.URL(key.toString).getHost
    val code= domain.hashCode % numParts
    if(code < 0) {
      code + numParts
    } else
      code
  }

  override def equals(other: scala.Any): Boolean = other match {
    case dnp: DomainCustomerPartitioner =>
      dnp.numPartitions == numParts
    case _ =>
      false
  }

}
