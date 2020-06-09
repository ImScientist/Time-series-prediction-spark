package ai.m5.challenge

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._


object preprocessing_prices {

  /**
    * Load some stages from a fitted Pipeline (PipelineModel)
    **/
  def getPipelineModel(spark: SparkSession, fitted_pipeline_path: String): PipelineModel = {

    val pipelineModelSales = PipelineModel.load(fitted_pipeline_path)

    val item_id_Indexer = pipelineModelSales.stages.filter(_.uid == "item_id")(0)
    val store_id_Indexer = pipelineModelSales.stages.filter(_.uid == "store_id")(0)

    val the_pipeline = new Pipeline().setStages(
      Array(item_id_Indexer, store_id_Indexer)
    )

    the_pipeline.fit(spark.emptyDataFrame)
  }

  def data_manipulation()(df: DataFrame): DataFrame = {

    val relevantCols = Seq(
      col("store_id_indexed").cast(ByteType),
      col("item_id_indexed").cast(ShortType),
      col("wm_yr_wk").cast(ShortType),
      col("sell_price").cast(FloatType)
    )

    df.select(relevantCols: _*)
  }

  def prices_preprocessing(spark: SparkSession, data_dir: String,
                           nrows: Int = -1): DataFrame = {

    val prices = spark.sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .load(data_dir + "/source/sell_prices.csv")
      .transform(utils.limitRows(nrows = nrows))

    val pipelineModel = getPipelineModel(
      spark = spark,
      fitted_pipeline_path = data_dir + "/trf/pipeline_sales")

    pipelineModel
      .transform(prices)
      .transform(data_manipulation())
  }

}
