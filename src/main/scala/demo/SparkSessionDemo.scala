package demo

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType

object SparkSessionDemo extends App {
  val sc = SparkCommonUtils.spContext
  val spark = SparkCommonUtils.spSession
  val path = SparkCommonUtils.datadir
  
  // Providing Explicit Schema.
  val schema = StructType(Array(
                           StructField("age",LongType,true),
                           StructField("deptid",LongType,true),
                           StructField("gender",StringType,true),
                           StructField("name",StringType,true),
                           StructField("salary",LongType,true)
  ))

  // val cust = spark.read.option("inferSchema",true).json(path + "customerData.json")
  
  
  var cust = spark.read.schema(schema).json(path + "customerData.json")
  cust = cust.toDF("e_age","e_deptid","e_gender","e_name","e_salary")
  cust.createOrReplaceTempView("customer")
  cust.show()
  // cust.groupBy("e_gender").count().show()
  // spark.sql("select e_gender,count(1) count from customer group by e_gender").show()
  
  
}