package cn.xinzoo.spark.ex

import org.apache.spark.sql.{Encoders, SparkSession}

object wordCount4sparkSession {

  def main(args: Array[String]): Unit = {

    if (args.length < 1) {
      System.err.println("Usage: wordCount4sparkSession <input path> [output path]")
      System.exit(1)
    }

    val spark = SparkSession.builder.
      appName("wordCount with sparkSession")
      .getOrCreate()
    //业务逻辑开始
    val textFile = spark.read.textFile(args(0))
    //For implicit conversions like converting RDDs to DataFrames
    //参看gethup官网dataset用法
    import spark.implicits._
    val wordCounts = textFile.flatMap(line => line.split(" "))
      .groupByKey(identity).count()
    if (args.length > 1)
      wordCounts.rdd.saveAsTextFile(args(1))
    else
      println(wordCounts.collect())
    //业务逻辑结束
    spark.stop()
  }
}
