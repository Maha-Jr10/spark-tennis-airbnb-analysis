import airbnb.AirbnbPriceEstimation
import atp.ATPWorldTourAnalysis
import org.apache.spark.sql.SparkSession
import tennis.ATPFinalTour

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("FinalProject")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println("\n=== Running Tennis Final ATP Tour (Part I.1) ===")
    ATPFinalTour.run(spark)
/*
    println("\n=== Running ATP World Tour 2020-2022 (Part I.2) ===")
    ATPWorldTourAnalysis.run(spark)

    println("\n=== Running Airbnb Price Estimation (Part II) ===")
    AirbnbPriceEstimation.run(spark)
*/
    println("Tests completed. Stopping Spark session.")

    spark.stop()
  }
}