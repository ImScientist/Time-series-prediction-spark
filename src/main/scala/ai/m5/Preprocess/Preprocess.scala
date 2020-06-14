package ai.m5.Preprocess

import org.apache.spark.sql.{DataFrame, SparkSession}

object Preprocess {
  /**
    * Data preprocessing;
    * - apply StringIndexer to all relevant columns
    * - specify DataType
    *
    * The relative order of the three preprocessing functions is imporntant
    * since `prices` transformation makes use of `sales` transformation
    * pipeline steps.
    *
    * @param data_dir   data directory with the following structure:
    *                   data_dir
    *                   |-- source (location of the original data)
    *                   |-- trf (location of the preprocessed data)
    **/
  def preprocess(spark: SparkSession, data_dir: String):
  Unit = {

    val calendar = Calendar.calendarPreprocessing(spark = spark, data_dir = data_dir)
    calendar.write.mode("overwrite").parquet(data_dir + "/trf/calendar.parquet")

    val sales = Sales.salesPreprocessing(spark = spark, data_dir = data_dir)
    sales.write.mode("overwrite").parquet(data_dir + "/trf/sales.parquet")

    val prices = Prices.pricesPreprocessing(spark = spark, data_dir = data_dir)
    prices.write.mode("overwrite").parquet(data_dir + "/trf/prices.parquet")
  }

  def preprocessAndMerge(spark: SparkSession,
                         data_dir: String,
                         preprocess_data: Boolean = true,
                         nrows: Int = -1): DataFrame = {

    if (preprocess_data) {
      preprocess(spark, data_dir)
    }

    Merge.mergeData(spark, data_dir, nrows)
  }

}
