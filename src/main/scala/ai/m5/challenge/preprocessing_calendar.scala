package ai.m5.challenge

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions.{to_date, dayofweek, dayofmonth, dayofyear, weekofyear, quarter, col, udf}
import org.apache.spark.sql.types._
import scala.collection.mutable
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.feature.StringIndexer


object preprocessing_calendar {

  def data_manipulation_1()(df: DataFrame): DataFrame = {

    val getSignal = udf((d: String) => {
      d.substring(2)
    }, StringType)

    df
      .withColumn("wm_yr_wk", col("wm_yr_wk").cast(ShortType))
      .withColumn("d", getSignal(col("d")).cast(ShortType))
      .withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
      .withColumn("month", col("month").cast(ByteType))
      .withColumn("year", col("year").cast(ShortType))
      .withColumn("day_in_week", dayofweek(col("date")).cast(ByteType))
      .withColumn("day_in_month", dayofmonth(col("date")).cast(ByteType))
      .withColumn("day_in_year", dayofyear(col("date")).cast(ShortType))
      .withColumn("week_in_yr", weekofyear(col("date")).cast(ByteType))
      .withColumn("quarter", quarter(col("date")).cast(ByteType))
      .withColumn("snap_CA", col("snap_CA").cast(ByteType))
      .withColumn("snap_TX", col("snap_TX").cast(ByteType))
      .withColumn("snap_WI", col("snap_WI").cast(ByteType))
      .na.fill("NA", Seq("event_name_1", "event_name_2", "event_type_1", "event_type_2"))
      .drop("weekday", "wday")
  }

  def init_calendar_pipeline(): Pipeline = {
    val event_name_1_Indexer = new StringIndexer()
      .setInputCol("event_name_1")
      .setOutputCol("event_name_1_indexed")

    val event_name_2_Indexer = new StringIndexer()
      .setInputCol("event_name_2")
      .setOutputCol("event_name_2_indexed")

    val event_type_1_Indexer = new StringIndexer()
      .setInputCol("event_type_1")
      .setOutputCol("event_type_1_indexed")

    val event_type_2_Indexer = new StringIndexer()
      .setInputCol("event_type_2")
      .setOutputCol("event_type_2_indexed")


    val stages = new mutable.ArrayBuffer[PipelineStage]()
    stages += event_name_1_Indexer
    stages += event_name_2_Indexer
    stages += event_type_1_Indexer
    stages += event_type_2_Indexer

    val pipeline = new Pipeline().setStages(stages.toArray)

    pipeline
  }

  def data_manipulation_2()(df: DataFrame): DataFrame = {

    df
      .drop("event_name_1", "event_name_2", "event_type_1", "event_type_2")
      .withColumn("event_name_1_indexed", col("event_name_1_indexed").cast(ByteType))
      .withColumn("event_name_2_indexed", col("event_name_2_indexed").cast(ByteType))
      .withColumn("event_type_1_indexed", col("event_type_1_indexed").cast(ByteType))
      .withColumn("event_type_2_indexed", col("event_type_2_indexed").cast(ByteType))
      .select(
        "date", "d", "wm_yr_wk",
        "year", "quarter", "month", "week_in_yr", "day_in_year", "day_in_month", "day_in_week",
        "snap_CA", "snap_TX", "snap_WI",
        "event_name_1_indexed", "event_name_2_indexed", "event_type_1_indexed", "event_type_2_indexed"
      )
  }

  def calendar_preprocessing(spark: SparkSession, data_dir: String): DataFrame = {

    val calendar = spark.sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .load(data_dir + "/source/calendar.csv")
      .transform(data_manipulation_1())

    val pipeline_calendar = init_calendar_pipeline()

    val pipelineModel = pipeline_calendar.fit(calendar)

    pipelineModel.write.overwrite().save(data_dir + "/trf/pipeline_calendar")

    pipelineModel
      .transform(calendar)
      .transform(data_manipulation_2())
  }


}
