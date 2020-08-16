package spark_joins
import demo.SparkCommonUtils
object Spark_Joins_SQL extends App {
  val sc = SparkCommonUtils.spContext
  val spark = SparkCommonUtils.spSession
  import spark.implicits._

  /*Payment Data.
   *+---------+----------+------+
    |paymentId|customerId|amount|
    +---------+----------+------+
    |        1|       101|  2500|
    |        2|       102|  1110|
    |        3|       103|   500|
    |        4|       104|   400|
    |        5|       105|   150|
    |        6|       106|   450|
    +---------+----------+------+
   *
   * Customer Data.
   * +----------+----+
    |customerId|name|
    +----------+----+
    |       101| Jon|
    |       102|Aron|
    |       103| Sam|
    +----------+----+

   */

  val payment = sc.parallelize(Seq(
    (1, 101, 2500),
    (2, 102, 1110),
    (3, 103, 500),
    (4, 104, 400),
    (5, 105, 150),
    (6, 106, 450))).toDF("paymentId", "customerId", "amount")
  

  val customer = sc.parallelize(Seq(
    (101, "Jon"),
    (102, "Aron"),
    (103, "Sam"))).toDF("customerId", "name")
    
  payment.createOrReplaceTempView("payment")
  customer.createOrReplaceTempView("customer")
  
  val result = spark.sql("""
    select p.paymentId, p.customerId, c.name
    from payment p join customer c
    on p.customerId = c.customerId
    """)
  
  result.write.format("hive").mode("append").saveAsTable("default.Demo")
  

}