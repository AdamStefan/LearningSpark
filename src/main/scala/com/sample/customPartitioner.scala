package com.sample


import org.apache.spark.Partitioner
import java.net

/**
  * Created by Stefan Adam on 6/13/2016.
  */
class ExactPartitioner[V](
                           partitions: Int)
  extends Partitioner {

  def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[Int]
    //  `k` is assumed to go continuously from 0 to elements-1.
    //  return k * partitions / elements

    return k
  }

  override def numPartitions: Int = {
    val noOfPartitions = this.partitions
    return noOfPartitions
  }

}

class DomainNamePartitioner(numParts:Int) extends  Partitioner {
  override def numPartitions: Int = numParts

  override def getPartition(key: Any): Int = {
    val domain = new java.net.URL(key.toString).getHost()
    val code = domain.hashCode % numPartitions
    if (code < 0) {
      return code + numPartitions // Make it non-negative
    } else {
      return code
    }
  }

  override def equals(other: Any): Boolean = other match {
    case dnp: DomainNamePartitioner => {
      return dnp.numPartitions == numPartitions
    }
    case _ =>
      false
  }
}






