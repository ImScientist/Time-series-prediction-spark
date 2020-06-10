package ai.m5

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.col
import org.scalatest.FunSpec

class FeaturesGenerationSpec
  extends FunSpec
    with DataFrameComparer
    with SparkSessionTestWrapper
    with ReadCsv {

  it("Test featuresGeneration()") {

    val input_data_Schema = StructType(
      Array(
        StructField("id_indexed", IntegerType, nullable = true),
        StructField("d", IntegerType, nullable = true),
        StructField("sales", DoubleType, nullable = true)
      ))

    val input_data = readCsv(
      inputSchema = input_data_Schema,
      fileLocation = "src/test/resources/featuresGeneration_input.csv",
      sp = spark
    )

    val columns_features = FeaturesGeneration.features_lags(column="sales", days_lag=Seq(1,2))
    val oiginal_features = input_data.columns.map(x => col(x))
    val result = input_data.select(oiginal_features ++ columns_features: _*)

    val expected_result_Schema = StructType(
      Array(
        StructField("id_indexed", IntegerType, nullable = true),
        StructField("d", IntegerType, nullable = true),
        StructField("sales", DoubleType, nullable = true),
        StructField("sales_lag_1", DoubleType, nullable = true),
        StructField("sales_lag_2", DoubleType, nullable = true)
      ))

    val expected_result = readCsv(
      inputSchema = expected_result_Schema,
      fileLocation = "src/test/resources/featuresGeneration_expected_result.csv",
      sp = spark
    )

    assertSmallDataFrameEquality(result, expected_result)

  }

}
