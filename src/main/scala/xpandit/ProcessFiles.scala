package xpandit

import org.apache.spark.sql.SparkSession
import xpandit.processors.{AverageRatingApp, BestAppsAnalysis, ExtendedAppMetrics, ExtendedAverageAppMetrics, GenreAppMetrics}

object ProcessFiles {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("App Dataframe Creation")
      .master("local[*]") // Change this to your cluster configuration if applicable
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY") // Set legacy datetime parser
      .getOrCreate()

    val df1 = AverageRatingApp.main(spark)
    BestAppsAnalysis.main(spark)
    val df3 = ExtendedAppMetrics.main(spark)
    ExtendedAverageAppMetrics.main(df1, df3)
    GenreAppMetrics.main(df3)

    spark.stop()
  }
}