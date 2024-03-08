package xpandit.processors

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, SparkSession}

object ExtendedAppMetrics {
  def main(spark: SparkSession): DataFrame = {

    val googlePlayStoreDF = spark.read
      .option("header", "true")
      .option("delimiter", ",")
      .csv("datasets/googleplaystore.csv")

    // Convert 'Size' column from string to double (value in MB)
    val dfWithSize = googlePlayStoreDF.withColumn("Size", regexp_replace(col("Size"), "M", "").cast(DoubleType))

    // Convert 'Price' column from string to double and present the value in euros
    val dfWithPrice = dfWithSize.withColumn("Price", regexp_replace(col("Price"), "\\$", "").cast(DoubleType) * 0.9)

    // Convert 'Last Updated' column from string to date
    val dfWithDate = dfWithPrice.withColumn("Last_Updated", to_date(col("Last Updated"), "MMM dd, yyyy"))

    // Rename columns
    val renamedColumnsDF = dfWithDate
      .withColumnRenamed("Content Rating", "Content_Rating")
      .withColumnRenamed("Current Ver", "Current_Version")
      .withColumnRenamed("Android Ver", "Minimum_Android_Version")

    // Group by 'App' and aggregate other columns
    val groupedDF = renamedColumnsDF
      .groupBy("App")
      .agg(
        collect_set("Category").alias("Categories"),
        max("Rating").alias("Rating"),
        max("Reviews").alias("Reviews"),
        max("Size").alias("Size"),
        max("Installs").alias("Installs"),
        max("Type").alias("Type"),
        max("Price").alias("Price"),
        max("Content_Rating").alias("Content_Rating"),
        collect_set("Genres").alias("Genres"),
        max("Last_Updated").alias("Last_Updated"),
        max("Current_Version").alias("Current_Version"),
        max("Minimum_Android_Version").alias("Minimum_Android_Version")
      )

    val df_3_converted = groupedDF
      .withColumn("Categories", concat_ws(",", col("Categories")))
      .withColumn("Genres", concat_ws(",", col("Genres")))


    // Save the DataFrame as a CSV file
    df_3_converted.write
      .option("header", "true")
      .csv("output/3_play_store_analysis")

    return df_3_converted
  } : DataFrame
}