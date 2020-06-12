package ai.m5

import ai.m5.Features.{FeaturesGeneration, FeaturesSelection}
import ai.m5.Preprocessing.{Calendar, Merge, Prices, Sales}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.ml.feature.{VectorAssembler, VectorIndexer}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.GBTRegressor
import org.apache.spark.ml.Pipeline


object Main {

  /**
    * Data preprocessing;
    * - apply StringIndexer to all relevant columns
    * - specify DataType
    *
    * @param data_dir data directory with the following structure:
    *                 data_dir
    *                   |-- source (location of the original data)
    *                   |-- trf (location of the preprocessed data)
    * @param store_data whether to store the data or not in data_dir/trf
    * */
  def preprocessing(spark: SparkSession, data_dir: String, store_data: Boolean = true):
  (DataFrame, DataFrame, DataFrame) = {

    val calendar = Calendar.calendarPreprocessing(spark = spark, data_dir = data_dir)
    val sales = Sales.salesPreprocessing(spark = spark, data_dir = data_dir)
    val prices = Prices.pricesPreprocessing(spark = spark, data_dir = data_dir)

    if (store_data) {
      calendar.write.mode("overwrite").parquet(data_dir + "/trf/calendar.parquet")
      sales.write.mode("overwrite").parquet(data_dir + "/trf/sales.parquet")
      prices.write.mode("overwrite").parquet(data_dir + "/trf/prices.parquet")
    }

    (calendar, sales, prices)
  }


  def cleanNans()(df: DataFrame): DataFrame = {

    val features = FeaturesSelection.getFeatures(df.columns)

    df.na.drop(cols = features).na.fill(0)
  }

  /**
    * Model training
    *
    * @param data_dir data directory with the following structure:
    *                 data_dir
    *                   |-- source (location of the original data)
    *                   |-- trf (location of the preprocessed data which we will use)
    * */
  def training(spark: SparkSession, data_dir: String, nrows: Int = -1): Unit = {

    //  spark.read.parquet(data_dir + "/trf/sales_grid.parquet")
    val df = Merge.mergeData(spark, data_dir, nrows)
      .transform(FeaturesGeneration.featuresGeneration())
      .transform(cleanNans())

    val features = FeaturesSelection.getFeatures(df.columns)
    val target = "sales"

    val assembler = new VectorAssembler(uid = "assembler")
      .setInputCols(features.toArray)
      .setOutputCol("features")

    val featureIndexer = new VectorIndexer(uid = "vectorIndexer")
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4)

    val gbt = new GBTRegressor(uid = "gradinetBooster")
      .setLabelCol(target)
      .setFeaturesCol("indexedFeatures")
      .setMaxIter(3)

    val indexingPipeline = new Pipeline().setStages(
      Array(assembler, featureIndexer)
    )

    val indexingPipelineModel = indexingPipeline.fit(df)

    // TODO: only the MML Gradient Boosting model makes use of the train, val, test splitting
    // TODO: use the MML library
    // val train = df.filter(col("d").between(0, 1913 - 28))
    // val valid = df.filter(col("d").between(1913 - 27, 1913))
    val train_valid = df.filter(col("d") <= 1913)
    val test = df.filter(col("d").between(1913 + 1, 1941))

    // In the second stage we have a VectorIndexerModel and it will not be affected by the fit method.
    val fullPipeline = new Pipeline().setStages(indexingPipelineModel.stages :+ gbt)

    // Train model.
    val model = fullPipeline.fit(train_valid)

    // Make predictions.
    val predictions = model.transform(test)

    // Select (prediction, true label) and compute test error.
    val evaluator = new RegressionEvaluator()
      .setLabelCol(target)
      .setPredictionCol("prediction")
      .setMetricName("rmse")

    val rmse = evaluator.evaluate(predictions)
    println(s"Root Mean Squared Error (RMSE) on test data = $rmse")

  }

}
