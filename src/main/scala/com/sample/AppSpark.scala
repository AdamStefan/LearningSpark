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
    val a = 1
    val conf = new SparkConf().setAppName("wordCount")
    conf.setMaster("local[*]")
    val sc = new SparkContext(conf)

//    testCombineByKey(sc)
//
//    testDegreeOfParallelism(sc)
//
//    testUsingGroupByKey(sc)
//    testJoins(sc)
//    test(sc)
//
//    val pageRankFile = "C://spark/data/mllib/pagerank_data.txt"
//
//    //computePageRank(pageRankFile, sc)
//    testAverage(sc)

    //saveSequenceFile(sc)

    loadcsvFile(sc)
  }


  private def test(sc: SparkContext): Unit = {

    val input = sc.textFile("//usr//local//spark//readme.md")

//    val input = sc.textFile("c://spark//readme.md")
    // Split it up into words.
    val words = input.flatMap(line => line.split(" "))
    // Transform into pairs and count.
    val counts = words.map(word => (word, 1)).reduceByKey { case (x, y) => x + y }
    var noOfItems = counts.count()
    val items = counts.top(4)

    //reduceToALine
    val reducedToALine = words.reduce((x, y) => x + y)
    println(reducedToALine)

    // test  with Serialization using a custom search function
    val youSearchFunction = new SearchFunctions("o")
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


  // understanding combine by key
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

  // test pageRank implementation
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


  // test compute averga
  private def testAverage(sparkContext: SparkContext): Unit = {
    val lines = sparkContext.textFile("//usr//local//spark//readme.md")
//    val lines = sparkContext.textFile("c://spark//readme.md")
    val dddd = lines.zipWithIndex().map(item => item._2.toDouble)
    val mean = dddd.sum() / dddd.count()
    println(mean)

    //var rangePart = new RangePartitioner(10,)
  }

  private def saveSequenceFile(sc:SparkContext):Unit= {
    val data = sc.parallelize(List(("key1",1),("key2",2),("key3",3)))
    data.saveAsSequenceFile("/tmp/seq-output2")








    //    "/user/hadoop/input"
    val data2 = sc.sequenceFile("/tmp/seq-output2",classOf[org.apache.hadoop.io.Text],classOf[IntWritable]).map{case(x,y)=>(x.toString,y.get())}
    val result = data2.collect()
    println(result.length)



  }

  private def loadcsvFile(sc:SparkContext):Unit= {

    val input = sc.textFile("/usr/local/spark/data/mllib/sample_tree_data.csv")
    val data = input.map(line => line.split(",").map(elem => elem.trim))
    //    val result = input.flatMap { case (_, txt) =>
    //      val reader = new CSVReader(new StringReader(txt));
    //      reader.readAll().map(x => Person(x(0), x(1)))
    //    }

    val sqlContext = new SQLContext(sc);
    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false") // Use first line of all files as header
      .load("/usr/local/spark/data/mllib/sample_tree_data.csv")

    df.show()

    df.select("_c0").show()
    print(df)
  }



//  JavaPairRDD<String, String> csvData = sc.wholeTextFiles(inputFile);
//  JavaRDD<String[]> keyedRDD = csvData.flatMap(new ParseLine());

}




