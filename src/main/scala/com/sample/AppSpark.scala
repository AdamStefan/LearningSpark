package com.sample

import org.apache.hadoop.io.IntWritable
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, RangePartitioner, SparkConf, SparkContext}

/**
  * Created by Stefan Adam on 6/10/2016.
  */
object AppSpark {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("wordCount")
    conf.setMaster("local[*]")
    val sc = new SparkContext(conf)


    val utilities = new Utilities()
    val wordsCountSampleFile = "Data\\wordCountExample.txt"
    val pageRankSampleFile = "Data\\pageRankSample.txt"
    val sensorDataFile = "Data\\sensorDataKmeans.txt"
    val outputFileModel = "Data\\kmeansModel"

    utilities.WordCount(sc,wordsCountSampleFile)
    utilities.computePageRank(sc,pageRankSampleFile)
    utilities.computeKmeans(sc,sensorDataFile,outputFileModel)

  }



  //test degree of parallelism
  private def testDegreeOfParallelism(sc: SparkContext): Unit = {
    val data = Seq(("a", 3), ("b", 4), ("a", 1))
    sc.parallelize(data).reduceByKey((x, y) => x + y) // Default parallelism
    sc.parallelize(data).reduceByKey((x, y) => x + y, 10) // Custom parallelism
  }

  // test using GroupByKey
  private def testUsingGroupByKey(sc: SparkContext): Unit = {
    val input = sc.textFile("//usr//local//spark//readme.md")

//    val input = sc.textFile("c://spark//readme.md")
    // Split it up into words.
    val words = input.flatMap(line => line.split(" "))
    val groupWords = words.groupBy(x => x).collect()
    val firstItem = groupWords(0)
    val firstItemVal = firstItem;
    val items = List(firstItemVal)
    val countsWord = words.map(word => (word, 1))

    val countsWordWithP = words.filter(item => item.startsWith("p")).map(word => (word, 1))
    val ret = countsWord.leftOuterJoin(countsWordWithP).collect()
    println(items(0))

  }

  // test join RDD
  private def testJoins(sc: SparkContext): Unit = {
    val input = sc.textFile("//usr//local//spark//readme.md")

//    val input = sc.textFile("c://spark//readme.md")
    // Split it up into words.
    val words = input.flatMap(line => line.split(" "))

    val countsWord = words.map(word => (word, 1))
    val countsWordWithP = words.filter(item => item.startsWith("p")).map(word => (word, 1))
    val ret = countsWord.join(countsWordWithP).collect()

    println(ret(2))

  }

  // test compute average
  private def testAverage(sparkContext: SparkContext): Unit = {
    val lines = sparkContext.textFile("//usr//local//spark//readme.md")
//    val lines = sparkContext.textFile("c://spark//readme.md")
    val dddd = lines.zipWithIndex().map(item => item._2.toDouble)
    val mean = dddd.sum() / dddd.count()
    println(mean)

    //var rangePart = new RangePartitioner(10,)
  }

  // test saveSequenceFile
  private def saveSequenceFile(sc:SparkContext):Unit= {
    val data = sc.parallelize(List(("key1",1),("key2",2),("key3",3)))
    data.saveAsSequenceFile("/tmp/seq-output2")

    val data2 = sc.sequenceFile("/tmp/seq-output2",classOf[org.apache.hadoop.io.Text],classOf[IntWritable]).map{case(x,y)=>(x.toString,y.get())}
    val result = data2.collect()
    println(result.length)

  }



}




