package cn.xinzoo.spark.ex

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object wordCount4sc {

  def main(args: Array[String]): Unit = {
    /*
      val conf = new SparkConf()
      .setAppName("wordCount with sc")
      val sc = new SparkContext(conf)
    */
    val spark = SparkSession.builder.
      appName("wordCount with wordCount4sc")
      .getOrCreate()
    val sc = spark.sparkContext

    //业务逻辑开始
    val textFile = sc.textFile(args(0))
    val wordCounts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1)).reduceByKey((a, b) => a + b)
    if (args.length > 1)
      wordCounts.saveAsTextFile(args(1))
    else
      println(wordCounts.collect())
    //业务逻辑结束

    sc.stop()
  }
}
