package xpandit.processors

import org.apache.spark.sql.functions.{col, isnan}
import org.apache.spark.sql.{DataFrame, SparkSession}

object BestAppsAnalysis {
  def main(spark: SparkSession): DataFrame = {

    val googlePlayStoreDF = spark.read
      .option("header", "true")
      .option("delimiter", ",")
      .csv("datasets/googleplaystore.csv")

    // Filter out rows with NaN values in the "Rating" column
    val filteredGooglePlayStoreDF = googlePlayStoreDF
      .filter(!col("Rating").isNull && !isnan(col("Rating").cast("float")))

    // Filter apps with Rating >= 4.0
    val bestAppsDF = filteredGooglePlayStoreDF
      .filter(filteredGooglePlayStoreDF("Rating").cast("float") >= 4.0)

    // Sort DataFrame in descending order based on Rating
    val sortedBestAppsDF = bestAppsDF
      .withColumn("Rating", col("Rating").cast("float"))
      .sort(col("Rating").desc)

    sortedBestAppsDF.write
      .option("header", "true")
      //.option("delimiter", "§")
      .csv("output/2_best_apps")

    return sortedBestAppsDF
  } : DataFrame
}