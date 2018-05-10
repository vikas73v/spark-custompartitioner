package com.vikas

import org.apache.spark.{RangePartitioner, SparkConf, SparkContext}

object CustomPartition {
  def main(args: Array[String]): Unit ={

    val sparkConf= new SparkConf().setAppName("CustomPartition").setMaster("local[2]").set("spark.eventLog.enabled","true")
    val sc1= new SparkContext(sparkConf)
    val ipfile= sc1.textFile("file:///C:\\vikas\\Documents\\software\\partition_data.txt")
    //val cal= ipfile.map(a => a.split('|')).map { case (v1) => ((v1(0), v1(1)), v1(2)) }

    val cal = ipfile.flatMap( x => x.split(" ")).map( x=> (x,1))

       /* println("&&&&&&&&&" +cal.getNumPartitions)
        cal.toDebugString.foreach(print)
        println("########################")*/

      //val partitionedData= cal.partitionBy(new MyCustomPartitioner(2) {} )
      val links = sc1.parallelize(List(
        ("http://com.victor", Seq("http://com.jane", "http://com.lily", "http://com.steve")),
        ("http://com.jane", Seq("http://com.victor", "http://com.lily", "http://com.ethan")),
        ("http://com.ethan", Seq("http://com.steve", "http://com.victor", "http://com.amine", "http://com.zoe")),
        ("http://com.zoe", Seq("http://com.amine", "http://com.ethan", "http://com.victor")),
        ("http://com.amine", Seq("http://com.lily", "http://com.zoe", "http://com.victor")),
        ("http://com.lily", Seq("http://com.victor", "http://com.jane")),
        ("http://com.jane", Seq("http://com.victor", "http://com.lily")),
        ("http://com.steve", Seq("http://com.victor", "http://com.amine"))
      ))


       val partitionedData= links.partitionBy(new DomainCustomerPartitioner(2) {})
      //val partitionedData= cal.partitionBy( new RangePartitioner(10,cal))
    val finalOut= partitionedData.mapPartitionsWithIndex{(partitionIndex ,dataIterator) =>dataIterator.map(dataInfo => dataInfo + " is located in  " + partitionIndex + " partition.")}

    finalOut.saveAsTextFile("file:///C:\\vikas\\Documents\\data\\data33")

  }
}
