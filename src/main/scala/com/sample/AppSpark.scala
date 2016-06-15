package com.sample

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, RangePartitioner, SparkConf, SparkContext}

/**
  * Created by Stefan Adam on 6/10/2016.
  */
object AppSpark {

  def main(args: Array[String]): Unit = {
    val a = 1
    val conf = new SparkConf().setAppName("wordCount")
    conf.setMaster("local[*]")
    val sc = new SparkContext(conf)

    //testCombineByKey(sc)

    //testDegreeOfParalellism(sc)

    //testUsingGroupByKey(sc)
    //testJoins(sc)
    //test(sc)

    val pageRankFile = "C://spark/data/mllib/pagerank_data.txt"

    //computePageRank(pageRankFile, sc)
    testAverage(sc)
  }


  private def test(sc: SparkContext): Unit = {

    val input = sc.textFile("c://spark//readme.md")
    // Split it up into words.
    val words = input.flatMap(line => line.split(" "))
    // Transform into pairs and count.
    val counts = words.map(word => (word, 1)).reduceByKey { case (x, y) => x + y }
    var noOfItems = counts.count()
    val items = counts.top(4)
    val reducedToALine = words.reduce((x, y) => x + y)
    println(reducedToALine)
    val youSearchFunction = new SearchFunctions("o")
    val wordsWithO = youSearchFunction.getMatchesNoReference(words)
    val firstWord = wordsWithO.first()
    val parallelizeCollection = sc.parallelize(List(1, 2, 3, 4))
    val sumSquareValues = parallelizeCollection.map(x => x * x).reduce((x, y) => x + y)

    val nlines = sc.parallelize(List("hello world", "hi"))
    val nwords = nlines.flatMap(line => line.split(" ")).collect()

    var wordsHashCode = words.map(word => (word.hashCode(), 1))
    //wordsHashCode.combineByKey()
    val el1 = new Tuple2(1, 2)
    val el2 = new Tuple2(3, 4)
    //sc.parallelize()


    println(nwords(2))



    val result = parallelizeCollection.map(x => x * x)
    result.persist(StorageLevel.DISK_ONLY)
    println(result.count())
    println(result.collect().mkString(","))




    //    println(sumSquareValues)
    //    println(firstWord)
    //    println(items(1)._1)
    //    println(items(1)._2)
    //    println(noOfItems)

    // Save the word count back out to a text file, causing evaluation.
    //counts.saveAsTextFile(outputFile)
    println("Ana are mere")
  }


  private def testCombineByKey(sc: SparkContext): Unit = {


    val scores = List(ScoreDetail("A", "Math", 98),
      ScoreDetail("A", "English", 98),
      ScoreDetail("B", "Math", 82),
      ScoreDetail("B", "English", 99),
      ScoreDetail("C", "Math", 94),
      ScoreDetail("C", "English", 89),
      ScoreDetail("D", "Math", 74),
      ScoreDetail("D", "English", 59))
    val scoresWithKey = for {i <- scores} yield (i.studentName, i)
    val scoresWithKeyRDD = sc.parallelize(scoresWithKey).partitionBy(new HashPartitioner(3)).cache()

    scoresWithKeyRDD.foreachPartition(partition => println(partition.length))

    val avgScoresRDD = scoresWithKeyRDD.combineByKey(
      (x: ScoreDetail) => (x.score, 1),
      (acc: (Float, Int), x: ScoreDetail) => (acc._1 + x.score, acc._2 + 1),
      (acc1: (Float, Int), acc2: (Float, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
      .map(
        {
          case (key, value) => (key, value._1 / value._2)
        }
      )

    val values = avgScoresRDD.collect().foreach(item => println(item._1 + " -" + item._2))


    val avgScoresRDD2 = scoresWithKeyRDD.combineByKey(
      (x: ScoreDetail) => (x.score, 1),
      (acc: (Float, Int), x: ScoreDetail) => (acc._1 * (acc._2 / acc._2 + 1) + (x.score / (acc._2 + 1)), acc._2 + 1),
      (acc1: (Float, Int), acc2: (Float, Int)) => (acc1._1 * acc1._2 / (acc1._2 + acc2._2) + acc1._2 * acc2._2 / (acc1._2 + acc2._2), acc1._2 + acc2._2))

    val values2 = avgScoresRDD2.collect().foreach(item => println(item._1 + " -" + item._2._1))


    val avgScoresRDD3 = scoresWithKeyRDD.combineByKey(
      (x: ScoreDetail) => (x.score, 1),
      (acc: (Float, Int), x: ScoreDetail) => (acc._1 + x.score, acc._2 + 1),
      (acc1: (Float, Int), acc2: (Float, Int)) => (acc2._1 * (acc2._2.toFloat / (acc1._2 + acc2._2).toFloat) + (acc1._1 / (acc1._2 + acc2._2)), acc1._2 + acc2._2)
    )


    val values3 = avgScoresRDD3.collect().foreach(item => println(item._1 + " - " + item._2._1 + " - " + item._2._2))

  }


  private def testDegreeOfParalellism(sc: SparkContext): Unit = {
    val data = Seq(("a", 3), ("b", 4), ("a", 1))
    sc.parallelize(data).reduceByKey((x, y) => x + y) // Default parallelism
    sc.parallelize(data).reduceByKey((x, y) => x + y, 10) // Custom parallelism
  }

  private def testUsingGroupByKey(sc: SparkContext): Unit = {

    val input = sc.textFile("c://spark//readme.md")
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

  private def testJoins(sc: SparkContext): Unit = {

    val input = sc.textFile("c://spark//readme.md")
    // Split it up into words.
    val words = input.flatMap(line => line.split(" "))

    val countsWord = words.map(word => (word, 1))
    val countsWordWithP = words.filter(item => item.startsWith("p")).map(word => (word, 1))
    val ret = countsWord.join(countsWordWithP).collect()

    println(ret(2))

  }

  private def computePageRank(file: String, sc: SparkContext): Unit = {

    val lines = sc.textFile(file)
    val links = lines.map(line => {
      val ds = line.split(" ")
      (ds(0), ds(1))
    }).distinct().groupByKey().partitionBy(new HashPartitioner(100)).persist()

    var ranks = links.map(item => (item._1, 1.0))


    for (index <- 1 to 20) {
      val contrib = links.join(ranks).flatMap {
        case (pageId, (rlinks, rank)) => {
          rlinks.map(dest => (dest, rank / rlinks.size))
        }
      }
      ranks = contrib.reduceByKey((x, y) => x + y).mapValues(v => 0.15 + 0.85 * v)
      ranks.foreach(item => println(item))
    }


    ranks.foreach(item => println(item))
  }

  private def testAverage(sparkContext: SparkContext): Unit = {
    val lines = sparkContext.textFile("c://spark//readme.md")
    val dddd = lines.zipWithIndex().map(item => item._2.toDouble)
    val mean = dddd.sum() / dddd.count()
    println(mean)

    //var rangePart = new RangePartitioner(10,)
  }

}
