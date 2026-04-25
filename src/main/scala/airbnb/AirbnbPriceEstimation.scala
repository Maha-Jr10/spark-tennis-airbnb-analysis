package airbnb

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.{StringIndexer, Imputer, OneHotEncoder, VectorAssembler, StandardScaler}
import org.apache.spark.ml.regression.GBTRegressor
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.evaluation.RegressionEvaluator

object AirbnbPriceEstimation {

  def run(spark: SparkSession): Unit = {
    import spark.implicits._

    // ==========================
    // 1. Load data
    // ==========================
    var df = spark.read.option("header", "true").option("inferSchema", "true")
      .csv("src/main/resources/airbnb-data.csv")
    println("Initial schema:")
    df.printSchema()
    df.describe().show()

    // ==========================
    // 2. Data cleaning & feature engineering
    // ==========================

    // Concatenate host_id and id (as required by project)
    df = df.withColumn("id", concat($"host_id", lit("_"), $"id")).drop("host_id")

    // Convert price: remove $ and , then to double
    df = df.withColumn("price", regexp_replace($"price", "[$,]", "").cast("double"))

    // Convert numeric columns
    df = df.withColumn("number_of_reviews", $"number_of_reviews".cast("int"))
      .withColumn("reviews_per_month", $"reviews_per_month".cast("double"))
      .withColumn("minimum_nights", $"minimum_nights".cast("int"))
      .withColumn("calculated_host_listings_count", $"calculated_host_listings_count".cast("double"))
      .withColumn("latitude", $"latitude".cast("double"))
      .withColumn("longitude", $"longitude".cast("double"))

    // Drop neighbourhood_group if present
    if (df.columns.contains("neighbourhood_group")) df = df.drop("neighbourhood_group")

    // Drop rows with nulls in essential categorical columns
    df = df.na.drop(Seq("neighbourhood", "room_type"))

    // ===== OUTLIER HANDLING =====
    // Cap price at 95th percentile (or filter out unrealistic values)
    val priceQuantiles = df.stat.approxQuantile("price", Array(0.95), 0.0)
    val priceCap = priceQuantiles(0)  // e.g., ~400-600 depending on data
    println(s"Capping price at 95th percentile: $priceCap")
    df = df.filter($"price" > 0 && $"price" <= priceCap)  // remove negative/zero and extreme highs

    // Cap minimum_nights (e.g., 365 is a year, but some listings have 1000+ which are likely errors)
    val minNightsCap = 365
    df = df.withColumn("minimum_nights", when($"minimum_nights" > minNightsCap, minNightsCap).otherwise($"minimum_nights"))

    // Cap calculated_host_listings_count (a host with > 1000 listings is suspicious)
    val hostListingsCap = 100
    df = df.withColumn("calculated_host_listings_count",
      when($"calculated_host_listings_count" > hostListingsCap, hostListingsCap)
        .otherwise($"calculated_host_listings_count"))

    // Apply log transformation to price (target) to reduce skewness
    df = df.withColumn("log_price", log($"price"))

    println(s"\nAfter preprocessing and outlier capping: ${df.count()} rows remaining")

    // ==========================
    // 3. Define feature columns
    // ==========================
    val categoricalCols = Array("neighbourhood", "room_type")
    val numericCols = Array(
      "number_of_reviews",
      "reviews_per_month",
      "availability_365",
      "minimum_nights",
      "calculated_host_listings_count",
      "latitude",
      "longitude"
    )

    // Index categorical columns
    val indexers = categoricalCols.map(c => new StringIndexer()
      .setInputCol(c)
      .setOutputCol(s"${c}_idx")
      .setHandleInvalid("keep"))

    // Impute numeric columns with median (more robust than mean)
    val imputers = numericCols.map(c => new Imputer()
      .setInputCol(c)
      .setOutputCol(s"${c}_imp")
      .setStrategy("mean"))

    // One-hot encode indexed categories
    val encoders = categoricalCols.map(c => new OneHotEncoder()
      .setInputCol(s"${c}_idx")
      .setOutputCol(s"${c}_vec"))

    // Assemble all features into a single vector
    val featureCols = numericCols.map(_ + "_imp") ++ categoricalCols.map(_ + "_vec")
    val assembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("features_raw")

    // Optional: scale features (helps gradient boosting convergence)
    val scaler = new StandardScaler()
      .setInputCol("features_raw")
      .setOutputCol("features")
      .setWithStd(true)
      .setWithMean(false)  // mean centering not necessary for trees, but can help

    // Model: Gradient Boosted Trees (often better than Random Forest)
    val gbt = new GBTRegressor()
      .setFeaturesCol("features")
      .setLabelCol("log_price")   // predict log(price)
      .setSeed(42)

    // ==========================
    // 4. Pipeline
    // ==========================
    val pipeline = new Pipeline().setStages(
      indexers ++ imputers ++ encoders :+ assembler :+ scaler :+ gbt
    )

    // ==========================
    // 5. Train / test split
    // ==========================
    val Array(train, test) = df.randomSplit(Array(0.8, 0.2), seed = 42)

    // ==========================
    // 6. Parameter grid (fewer options for speed, but can be expanded)
    // ==========================
    val paramGrid = new ParamGridBuilder()
      .addGrid(gbt.maxIter, Array(30, 50))
      .addGrid(gbt.maxDepth, Array(5, 7))
      .addGrid(gbt.stepSize, Array(0.05, 0.1))
      .build()

    val evaluator = new RegressionEvaluator()
      .setLabelCol("log_price")
      .setMetricName("rmse")

    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEstimatorParamMaps(paramGrid)
      .setEvaluator(evaluator)
      .setNumFolds(3)
      .setSeed(42)

    println("\nTraining model with cross-validation (3 folds) on log(price)...")
    val cvModel = cv.fit(train)

    // Best cross-validated RMSE on log scale
    val avgMetrics = cvModel.avgMetrics
    val bestIndex = avgMetrics.zipWithIndex.minBy(_._1)._2
    val bestCV_RMSE_log = avgMetrics(bestIndex)
    println(s"Best cross-validated RMSE (log scale): $bestCV_RMSE_log")

    // Predict on test set (predictions are in log space)
    val predictionsLog = cvModel.transform(test)

    // Convert predictions back to original price scale
    val predictions = predictionsLog
      .withColumn("predicted_price", exp($"prediction"))
      .withColumn("actual_price", exp($"log_price"))

    // Evaluate on original price scale
    val originalPriceEvaluator = new RegressionEvaluator()
      .setLabelCol("actual_price")
      .setPredictionCol("predicted_price")
      .setMetricName("rmse")

    val testRMSE = originalPriceEvaluator.evaluate(predictions)
    println(s"Test RMSE (original price scale): $testRMSE")

    // Also compute mean absolute error for interpretability
    val maeEvaluator = new RegressionEvaluator()
      .setLabelCol("actual_price")
      .setPredictionCol("predicted_price")
      .setMetricName("mae")
    val testMAE = maeEvaluator.evaluate(predictions)
    println(s"Test MAE (original price scale): $testMAE")

    // Show sample predictions (actual vs predicted)
    println("\nSample predictions vs actual (test set, original prices):")
    predictions.select("actual_price", "predicted_price")
      .withColumnRenamed("actual_price", "price")
      .withColumnRenamed("predicted_price", "prediction")
      .show(10, false)
  }
}