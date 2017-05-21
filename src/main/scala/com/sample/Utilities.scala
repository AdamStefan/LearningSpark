package com.sample

import org.apache.spark.sql.SQLContext
import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors

/**
  * Created by Stefan on 5/20/2017.
  */
class Utilities {

  def WordCount(sc: SparkContext, filePath: String ): Unit = {

    val input = sc.textFile(filePath)

    // Split it up into words.
    val words = input.flatMap(line => line.split(" "))

    // Transform into pairs and count.
    val counts = words.map(word => (word, 1)).reduceByKey { case (x, y) => x + y }
    var noOfItems = counts.count()
    println("Number of words is:",noOfItems)
    val items = counts.top(4)

    //reduceToALine
    val reducedToALine = words.reduce((x, y) => x + y)
    println(reducedToALine)

    // test  with Serialization using a custom search function
    val youSearchFunction = new SearchFunctions("mer")
    val wordsWithO = youSearchFunction.getMatchesNoReference(words)
    val firstWord = wordsWithO.first()


    // use parallelize collection
    val parallelizeCollection = sc.parallelize(List(1, 2, 3, 4))
    val sumSquareValues = parallelizeCollection.map(x => x * x).reduce((x, y) => x + y)

    val nlines = sc.parallelize(List("hello world", "hi"))
    val nwords = nlines.flatMap(line => line.split(" ")).collect()


    var wordsHashCode = words.map(word => (word.hashCode(), 1))

    //use persist to store a RDD
    val result = parallelizeCollection.map(x => x * x)
    result.persist(StorageLevel.DISK_ONLY)
    println(result.count())
    println(result.collect().mkString(","))
  }

  // pageRank implementation
  def computePageRank(sc: SparkContext, filePath: String): Unit = {

    val lines = sc.textFile(filePath)
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

  // compute KMeans
  def computeKmeans(sc:SparkContext, file:String, outputFile: String):Unit= {

    //val data = sc.textFile("C:\\WorkingProjects\\kmeans_data.txt")
    val data = sc.textFile(file)
    val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()

    // Cluster the data into two classes using KMeans
    val numClusters = 2
    val numIterations = 20
    val clusters = KMeans.train(parsedData, numClusters, numIterations)


    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(parsedData)

    val firstCenter = clusters.clusterCenters(0)
    val secondCenter = clusters.clusterCenters(1)
    println(firstCenter)
    println(secondCenter)

    println("Within Set Sum of Squared Errors = " + WSSSE)

    // Save and load model
    //clusters.save(sc, outputFile)
    //val sameModel = KMeansModel.load(sc, outputFile)
  }

  // understanding combine by key
  def testCombineByKey(sc: SparkContext): Unit = {


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
      (x: ScoreDetail) => (x.score, 1), // combiner
      (acc: (Float, Int), x: ScoreDetail) => (acc._1 + x.score, acc._2 + 1), // merge values step
      (acc1: (Float, Int), acc2: (Float, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)) // merge combiners steps
      .map(
      {
        case (key, value) => (key, value._1 / value._2)
      }
    )

    val values = avgScoresRDD.collect().foreach(item => println(item._1 + " -" + item._2))

  }


  def loadcsvFile(sc:SparkContext, path:String):Unit= {
    val input = sc.textFile("/usr/local/spark/data/mllib/sample_tree_data.csv")
    val data = input.map(line => line.split(",").map(elem => elem.trim))


    val sqlContext = new SQLContext(sc);
    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false") // Use first line of all files as header
      .load(path)

    df.show()

    df.select("_c0").show()
    print(df)
  }



}
