package ml

import demo.SparkCommonUtils

object LinearRegressionExample extends App {
  import org.apache.spark.ml.evaluation.RegressionEvaluator
  import org.apache.spark.ml.regression.LinearRegression
  import org.apache.spark.ml.tuning.{ ParamGridBuilder, TrainValidationSplit }
  import org.apache.log4j._
  import org.apache.spark.ml.feature.VectorAssembler
  import org.apache.spark.mllib.linalg.Vectors

  val spContext = SparkCommonUtils.spContext
  val spark = SparkCommonUtils.spSession
  import spark.implicits._

  Logger.getLogger("org").setLevel(Level.ERROR)
  val dataDir = "data-files\\"

  // Load input data.
  val ecommDF = spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv(dataDir + "EcommerceCustomers.csv")

  //ecommDF.show()
  //ecommDF.printSchema()

  val ecommDF1 = ecommDF.select(ecommDF("Yearly Amount Spent").as("label"), $"Avg Session Length", $"Time on App", $"Time on Website", $"Length of Membership")
  ecommDF1.show(5,false)

  val inputCols = Array("Avg Session Length", "Time on App", "Time on Website", "Length of Membership")

  val assembler = new VectorAssembler().setInputCols(inputCols).setOutputCol("features")

  val ecommDF2 = assembler.transform(ecommDF1).select($"label", $"features")

  ecommDF2.show(5, false)

  // Create object of LinearRegression.
  val lr = new LinearRegression()

  // Provide the training data.
  val lrModel = lr.fit(ecommDF2)
  
  println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

  val trainingSummary = lrModel.summary
  println(s"numIterations: ${trainingSummary.totalIterations}")
  println(s"objectiveHistory: ${trainingSummary.objectiveHistory.toList}")
  trainingSummary.residuals.show()

  // Model Evaluation.
  println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
  println(s"MSE: ${trainingSummary.meanSquaredError}")
  println(s"r2: ${trainingSummary.r2}")

}