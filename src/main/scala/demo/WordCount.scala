package demo

object WordCount extends App{
  val sc = SparkCommonUtils.spContext
  val spark = SparkCommonUtils.spSession
  val path = SparkCommonUtils.datadir
  
  val rdd = sc.textFile(path + "hellospark_ip")
  val x = rdd.map(ele => (ele,1))
  val y = x.reduceByKey((x,y) => x+y)
  val z = y.collect()
  z.foreach(println)
  
}