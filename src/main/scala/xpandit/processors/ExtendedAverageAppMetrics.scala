package xpandit.processors

import org.apache.spark.sql.DataFrame

object ExtendedAverageAppMetrics {
  def main(df1: DataFrame, df3: DataFrame): DataFrame = {

    // Join the DataFrames on the "App" column
    val dfCleaned = df3.join(df1, Seq("App"), "left_outer")

    dfCleaned.write
      .option("compression", "gzip")
      .parquet("output/4_googleplaystore_cleaned")

    return dfCleaned
  } : DataFrame
}