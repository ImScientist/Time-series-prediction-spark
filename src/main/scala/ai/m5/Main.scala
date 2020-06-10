package ai.m5

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.ml.feature.{VectorAssembler, VectorIndexer}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.GBTRegressor
import org.apache.spark.ml.Pipeline


object Main {

  def preprocessing(spark: SparkSession, data_dir: String, store_data: Boolean = true):
  (DataFrame, DataFrame, DataFrame) = {

    val calendar = PreprocessingCalendar.calendar_preprocessing(spark = spark, data_dir = data_dir)
    val sales = PreprocessingSales.sales_preprocessing(spark = spark, data_dir = data_dir)
    val prices = PreprocessingPrices.prices_preprocessing(spark = spark, data_dir = data_dir)

    if (store_data) {
      calendar.write.mode("overwrite").parquet(data_dir + "/trf/calendar.parquet")
      sales.write.mode("overwrite").parquet(data_dir + "/trf/sales.parquet")
      prices.write.mode("overwrite").parquet(data_dir + "/trf/prices.parquet")
    }

    //    val sales_grid = PreprocessingMerge.merge_data(spark=spark, data_dir=data_dir, nrows=4)

    (calendar, sales, prices)
  }


  def training(spark: SparkSession, data_dir: String): Unit = {

    val df = spark
      .read.parquet(data_dir + "/trf/sales_grid.parquet")
      .transform(FeaturesGeneration.featuresGeneration())

    val features = FeaturesSelection.getFeatures(df.columns)
    val target = "sales"

    val assembler = new VectorAssembler()
      .setInputCols(features.toArray)
      .setOutputCol("features")

    val transformed = assembler
      .transform(df.na.drop(cols=features).na.fill(0))
      .cache()

    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4)
      .fit(transformed)

    // TODO: only the MML Gradient Boosting model makes use of the train, val, test splitting
    // TODO: use the MML library
    // val train = transformed.filter(col("d").between(0, 1913 - 28))
    // val valid = transformed.filter(col("d").between(1913 - 27, 1913))
    val train_valid = transformed.filter(col("d") <= 1913)
    val test = transformed.filter(col("d").between(1913 + 1, 1941))
    // val Array(trainingData, testData) = transformed.randomSplit(Array(0.7, 0.3))

    // Train a GBT model.
    val gbt = new GBTRegressor()
      .setLabelCol(target)
      .setFeaturesCol("indexedFeatures")
      .setMaxIter(3)

    val pipeline = new Pipeline()
      .setStages(Array(featureIndexer, gbt))

    // Train model. This also runs the indexer.
    val model = pipeline.fit(train_valid)

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
