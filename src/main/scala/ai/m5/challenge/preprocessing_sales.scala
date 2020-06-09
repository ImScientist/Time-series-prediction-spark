package ai.m5.challenge

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._


object preprocessing_sales {

  def init_sales_pipeline(): Pipeline = {

    val id_Indexer = new StringIndexer(uid = "id")
      .setInputCol("id")
      .setOutputCol("id_indexed")

    val item_id_Indexer = new StringIndexer("item_id")
      .setInputCol("item_id")
      .setOutputCol("item_id_indexed")

    val dept_id_Indexer = new StringIndexer("dept_id")
      .setInputCol("dept_id")
      .setOutputCol("dept_id_indexed")

    val cat_id_Indexer = new StringIndexer("cat_id")
      .setInputCol("cat_id")
      .setOutputCol("cat_id_indexed")

    val store_id_Indexer = new StringIndexer("store_id")
      .setInputCol("store_id")
      .setOutputCol("store_id_indexed")

    val state_id_Indexer = new StringIndexer("state_id")
      .setInputCol("state_id")
      .setOutputCol("state_id_indexed")


    val pipeline = new Pipeline().setStages(
      Array(
        id_Indexer,
        item_id_Indexer,
        dept_id_Indexer,
        cat_id_Indexer,
        store_id_Indexer,
        state_id_Indexer
      )
    )

    pipeline
  }

  def data_manipulation()(df: DataFrame): DataFrame = {

    val d_Cols = df.columns.filter(_.slice(0, 2) == "d_")

    val relevantCols = Seq(
      col("id_indexed").cast(ShortType),
      col("item_id_indexed").cast(ShortType),
      col("dept_id_indexed").cast(ByteType),
      col("cat_id_indexed").cast(ByteType),
      col("store_id_indexed").cast(ByteType),
      col("state_id_indexed").cast(ByteType)) ++
      d_Cols.map(x => col(x).cast(FloatType))

    df.select(relevantCols: _*)
  }

  def sales_preprocessing(spark: SparkSession, data_dir: String,
                          nrows: Int = -1): DataFrame = {

    val sales = spark.sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .load(data_dir + "/source/sales_train_evaluation.csv")
      .transform(utils.limitRows(nrows=nrows))

    val pipeline_sales = init_sales_pipeline()

    val pipelineModel = pipeline_sales.fit(sales)

    pipelineModel.write.overwrite().save(data_dir + "/trf/pipeline_sales")

    pipelineModel
      .transform(sales)
      .transform(data_manipulation())
  }


}
