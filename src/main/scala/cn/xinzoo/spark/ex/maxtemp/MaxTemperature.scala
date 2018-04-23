package cn.xinzoo.spark.ex.maxtemp

import org.apache.spark.sql.SparkSession

object MaxTemperature {

  def main(args: Array[String]): Unit = {

    val MISSING = 9999

    if (args.length ne 2) {
      System.err.println("Usage:MaxTemperature <input path> <output path>")
      System.exit(-1)
    }

    val spark  = SparkSession.builder().
      appName("spark in MaxTemperature").
      getOrCreate()
    //val weaDataSet = spark.read.textFile("ncdc-input")
    //val MISSING = 9999
    val weaDataSet =  spark.read.textFile(args(0))
    import spark.implicits._

    val kv = for {
      line:String <- weaDataSet

      temp = line.substring(87,92).toInt
      year = line.substring(15,19)
      quality = line.substring(92,93)
    } yield if (temp != MISSING && quality.matches("[01459]")) {
      (year,temp)
    } else (year,Integer.MIN_VALUE)

    val maxTemperature = kv.rdd.flatMap(_,_)
      //.flatMap(_,_).reduce(Math.max(_,_))
    //maxTemperature.报错
    if (args.length > 1)   //测试新的get
      maxTemperature.saveAsTextFile(args(1))
    else
      println(maxTemperature.collect())
    //业务逻辑结束

    sc.stop()
    //kv.groupBy()
  }
}
