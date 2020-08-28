package streaming
object SparkStreamingDemo extends App {

  import demo._

  val spContext = SparkCommonUtils.spContext
  val datadir = SparkCommonUtils.datadir
  import org.apache.spark.streaming.StreamingContext
  import org.apache.spark.streaming.StreamingContext._
  import org.apache.spark.streaming.Seconds
  import org.apache.spark.rdd.RDD
  import java.io._
  import org.apache.log4j.Logger
  import org.apache.log4j.Level;

  Logger.getLogger("org").setLevel(Level.ERROR);
  Logger.getLogger("akka").setLevel(Level.ERROR);

  //Create streaming context with latency of 5 seconds.
  var ssc = new StreamingContext(spContext, Seconds(5))

  val lines = ssc.socketTextStream("localhost", 9999)
  lines.print()

  val logFile = new File(datadir + "streamingoutput.txt")
  logFile.delete()
  val pw = new PrintWriter(logFile)

  //Word count within RDD
  val words = lines.flatMap(x => x.split(" "))

  val pairs = words.map(x => (x, 1))

  val wordCounts = pairs.reduceByKey((x, y) => x + y)

  wordCounts.print()

  
  //Count lines
  def computeMetrics(rdd: RDD[String]): Long = {
    val linesCount = rdd.count()
    print("Lines in RDD :" + linesCount + "\n")
    pw.write("Lines in RDD :" + linesCount + "\n")
    pw.flush()
    return linesCount
  }

  val lineCount = lines.foreachRDD(computeMetrics(_))

 
  //Compute window metrics
  def windowMetrics(rdd: RDD[String]) = {
    print("Window RDD size:" + rdd.count() + "\n")
    pw.write("Window RDD size:" + rdd.count() + "\n")
    pw.flush();
  }

  val windowedRDD = lines.window(Seconds(15), Seconds(5))

  windowedRDD.foreachRDD(windowMetrics(_))
 
  ssc.start()
  ssc.awaitTermination()
  //  pw.close()
  //ssc.stop()

}