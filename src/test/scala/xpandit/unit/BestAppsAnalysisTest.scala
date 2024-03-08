package xpandit.unit

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.functions.col
import xpandit.processors.BestAppsAnalysis

class BestAppsAnalysisTest extends AnyFunSuite {

  test("Test BestAppsAnalysis main method") {
    val spark = SparkSession.builder()
      .master("local[2]") // Use 2 cores for testing
      .appName("TestBestAppsAnalysis")
      .getOrCreate()

    // Create a test DataFrame with sample data
    import spark.implicits._
    val testData = Seq(
      ("App1", 4.5),
      ("App2", 3.8),
      ("App3", 4.2),
      ("App4", null),  // Simulate a row with null Rating
      ("App5", 4.7)
    ).toDF("App", "Rating")

    // Call the main method of BestAppsAnalysis object
    val resultDF: DataFrame = BestAppsAnalysis.main(spark)

    // Assert that the result DataFrame is not null
    assert(resultDF != null)

    // Assert that the result DataFrame contains only apps with Rating >= 4.0
    assert(resultDF.select(col("Rating")).collect().forall(_.getDouble(0) >= 4.0))

    // Stop SparkSession after the test
    spark.stop()
  }
}
