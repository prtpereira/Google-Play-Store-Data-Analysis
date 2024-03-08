package xpandit.processors

import org.apache.spark.sql.functions.{avg, col, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

object AverageRatingApp {
  def main(spark: SparkSession): DataFrame = {

    val userReviewsDF = spark.read
      .option("header", "true")
      .option("delimiter", ",")
      .csv("datasets/googleplaystore_user_reviews.csv")

    // Filter out rows with NaN values in Sentiment_Polarity column
    val filteredUserReviewsDF = userReviewsDF.filter(!col("Sentiment_Polarity").cast("double").isNaN)

    val df1 = filteredUserReviewsDF.groupBy("App")
      .agg(avg("Sentiment_Polarity").alias("Average_Sentiment_Polarity"))
      .withColumn("Average_Sentiment_Polarity", when(col("Average_Sentiment_Polarity").isNull, 0.0)
        .otherwise(col("Average_Sentiment_Polarity")))

    df1.write
      .option("header", "true")
      .csv("output/1_average_sentiment_polarity_per_app")

    return df1
  } : DataFrame
}