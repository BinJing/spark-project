package ml
object SparkMlRecommendations extends App {

  import demo._
  import org.apache.spark.sql.functions._
  import org.apache.log4j.Logger
  import org.apache.log4j.Level;

  val spSession = SparkCommonUtils.spSession
  val spContext = SparkCommonUtils.spContext
  val datadir = SparkCommonUtils.datadir

  val ratingsDf = spSession.read.option("inferSchema",true).csv(datadir + "useritemdata.txt").toDF("user","item","rating")
  ratingsDf.show()
  ratingsDf.printSchema()


  //Split into training and testing data
  /**
   *  I would recommend that if you have a reasonable amount of data that you split into three data sets,
   *  "training" which you run the ML algorithms on; "test", which you use to check how your
   *  training is going; and "validation" which you never use until you think your entire ML process is optimized.
   */
  val Array(trainingData, testData) = ratingsDf.randomSplit(Array(0.9, 0.1))
  // trainingData: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row]
  // testData: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row]

  trainingData.count()
  testData.count()

  import org.apache.spark.ml.recommendation.ALS
  // https://spark.apache.org/docs/1.1.0/api/java/org/apache/spark/mllib/recommendation/ALS.html

  // Build the recommendation model using ALS on the training data.
  val als = new ALS()
  // als: org.apache.spark.ml.recommendation.ALS

  als.setRank(10) // Set the rank of the feature matrices computed (number of features). Default: 10.
  als.setMaxIter(5) // Max no of iterations to run.
  als.setUserCol("user")
  als.setItemCol("item")
  als.setRatingCol("rating")

  val model = als.fit(trainingData)
  // model: org.apache.spark.ml.recommendation.ALSModel = als_31deb09f73f6

  val predictions = model.transform(testData) // DataFrame

  println("Recommendation scores:")
  predictions.select("user", "item", "prediction").show()

}