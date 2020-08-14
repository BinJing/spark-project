package demo

object SparkCommonUtils {

  import org.apache.spark.sql.SparkSession

  import org.apache.spark.SparkContext

  import org.apache.spark.SparkConf

  //Directory where the data files for the examples exist.
  // val datadir = "data-files\\"
  val datadir = "data-files/"

  //A name for the spark instance. Can be any string
  val appName = "App-Prakash"

  //Pointer / URL to the Spark instance - embedded
  val sparkMasterURL = "local[10]"
  //val sparkMasterURL = "spark://192.168.56.1:7077" // Spark Standalone
  //val sparkMasterURL = "yarn"

  //Temp dir required for Spark SQL
  // val tempDir = "file:///c:/temp/spark-warehouse"
  val tempDir = "/home/edureka/temp"

  var spSession: SparkSession = null
  var spContext: SparkContext = null

  //Initialization. Runs when object is created
  {
    //Need to set hadoop.home.dir to avoid errors during startup. Required for Windows machine.
    // System.setProperty("hadoop.home.dir", "C:\\winutils");

    //Create spark configuration object
    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster(sparkMasterURL)
      .set("spark.executor.memory", "2g")
      .set("spark.sql.shuffle.partitions", "2")

    //Get or create a spark context. Creates a new instance if not exists	
    spContext = SparkContext.getOrCreate(conf)

    //Create a spark SQL session
    spSession = SparkSession
      .builder()
      .appName(appName)
      .master(sparkMasterURL)
      .config("spark.sql.warehouse.dir", tempDir)
      .getOrCreate()

  }
}