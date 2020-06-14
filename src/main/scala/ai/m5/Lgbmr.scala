package ai.m5

import ai.m5.Features.{FeaturesGeneration, FeaturesSelection}
import ai.m5.Gbtr.cleanNans
import ai.m5.Preprocess.Preprocess.preprocessAndMerge
import com.microsoft.ml.spark.lightgbm.LightGBMRegressor
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{VectorAssembler, VectorIndexer}
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col


object Lgbmr {

  def training(spark: SparkSession,
               data_dir: String,
               preprocess: Boolean = true,
               nrows: Int = -1): Unit = {

    //  spark.read.parquet(data_dir + "/trf/sales_grid.parquet")
    val df = preprocessAndMerge(spark, data_dir, preprocess, nrows)
      .transform(FeaturesGeneration.featuresGeneration())
      .transform(cleanNans())

    val features = FeaturesSelection.getFeatures(df.columns)
    val labelCol = "sales"
    val predicitonCol = "prediction"

    val assembler = new VectorAssembler(uid = "assembler")
      .setInputCols(features.toArray)
      .setOutputCol("features")

    val featureIndexer = new VectorIndexer(uid = "vectorIndexer")
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4)

    val lgbm = new LightGBMRegressor()
      .setObjective("poisson")
      .setFeaturesCol("indexedFeatures")
      .setLabelCol(labelCol)
      .setPredictionCol(predicitonCol)
      .setNumLeaves(5)
      .setNumIterations(10)

    val indexingPipeline = new Pipeline().setStages(
      Array(assembler, featureIndexer)
    )

    val indexingPipelineModel = indexingPipeline.fit(df)

    val train_valid = df.filter(col("d") <= 1913)
    val test = df.filter(col("d").between(1913 + 1, 1941))

    val paramGrid = new ParamGridBuilder()
      .addGrid(lgbm.numLeaves, Array(5, 10))
      .addGrid(lgbm.lambdaL2, Array(0.1, 0.3))
      .build()

    val evaluator = new RegressionEvaluator()
      .setLabelCol(labelCol)
      .setPredictionCol(predicitonCol)
      .setMetricName("rmse")

    val gridSearchEstimator = new TrainValidationSplit()
      .setEstimator(lgbm)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setTrainRatio(0.8)
      .setParallelism(2)

    // In the second stage we have a VectorIndexerModel and
    // it will not be affected by the fit method.
    val fullPipeline = new Pipeline().setStages(
      indexingPipelineModel.stages :+ gridSearchEstimator
    )

    // Train model.
    val model = fullPipeline.fit(train_valid)

    // Make predictions.
    val predictions = model.transform(test)

    val rmse = evaluator.evaluate(predictions)
    println(s"Root Mean Squared Error (RMSE) on test data = $rmse")
  }

}
