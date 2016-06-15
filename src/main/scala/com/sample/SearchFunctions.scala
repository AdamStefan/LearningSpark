package com.sample

import org.apache.spark.rdd.RDD

/**
  * Created by Stefan Adam on 6/10/2016.
  */
class SearchFunctions(val query: String) extends Serializable {

  def isMatch(s: String): Boolean = {
    s.contains(query)
  }

  def getMatchesFunctionReference(rdd: RDD[String]): RDD[Boolean] = {
    rdd.map(x => isMatch(x))
  }

  def getMatchesFieldReference(rdd: RDD[String]): RDD[Array[String]] = {
    // Problem: "query" means "this.query", so we pass all of "this"
    rdd.map(x => x.split(this.query))
  }

  def getMatchesNoReference(rdd: RDD[String]): RDD[Array[String]] = {
    val query_ = this.query
    rdd.map(x => x.split(query))
  }

}
