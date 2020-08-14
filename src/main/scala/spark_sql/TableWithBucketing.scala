package spark_sql

import demo.SparkCommonUtils
import org.apache.spark.sql.SaveMode

object TableWithBucketing extends App {
  val spark = SparkCommonUtils.spSession
  import spark.implicits._
  val sc = SparkCommonUtils.spContext
  spark.sparkContext.setLogLevel("ERROR")
  
  val people = spark.read.json("data-files/people.json")
  spark.sql("create database  mydb")
  people.write.bucketBy(2, "name").sortBy("age").format("parquet").saveAsTable("mydb.people_bucketed")
  //people.write.format("parquet").saveAsTable("people_bucketed")
}