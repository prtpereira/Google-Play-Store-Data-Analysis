package xpandit.processors

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{avg, col, count, split}

object GenreAppMetrics {
  def main(df_3: DataFrame): DataFrame = {

    val df_4 = df_3.withColumn("Genres", split(col("Genres"), ";"))
      .groupBy("Genres")
      .agg(
        count("App").alias("Count"),
        avg("Rating").alias("Average_Rating")
      )

    df_4.write
      .format("parquet")
      .option("compression", "gzip")
      .save("output/5_googleplaystore_metrics")

    return df_4
  } : DataFrame
}