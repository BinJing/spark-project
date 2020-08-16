package spark_sql

import demo.SparkCommonUtils

object CCA175 extends App{
  val spark = SparkCommonUtils.spSession
  import spark.implicits._
  val sc = SparkCommonUtils.spContext
  spark.sparkContext.setLogLevel("ERROR")
  val path = SparkCommonUtils.datadir
  
  var custCSV = spark.read.option("header",true).csv(path + "customer.csv")
  
  custCSV = custCSV.filter($"name".startsWith("A"))
  
/*  custCSV.write.parquet("custmer_temp_parquet")
  custCSV.write.orc("")
  custCSV.write.json("")
  custCSV.write.csv("")
  custCSV.write.format("hive").saveAsTable("")*/
  
  spark.read.parquet(path + "people.parquet").show()
  spark.read.json(path +"customerData.json").show()
  spark.read.text(path +"people.txt").show()
}