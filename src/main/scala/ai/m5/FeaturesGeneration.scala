package ai.m5

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._

object FeaturesGeneration {

  val idWindow: WindowSpec = Window
    .partitionBy(col("id_indexed"))
    .orderBy(col("d"))

  def RollingWindow(size: Int = 0, lag: Int = 0): WindowSpec = {

    val upperBound = if (lag > 0) -lag else Window.currentRow
    val lowerBound = if (size > 0) -lag - (size - 1) else Window.unboundedPreceding

    Window
      .partitionBy(col("id_indexed"))
      .orderBy(col("d"))
      .rowsBetween(lowerBound, upperBound)
  }

  def features_lags(column: String, days_lag: Seq[Int]): Seq[Column] = {

    days_lag.map(x => {
      lag(col(column), x).over(idWindow).alias("%s_lag_%d".format(column, x))
    })
  }

  def features_differences_with_lags(column: String, days_lag: Seq[Int]): Seq[Column] = {

    days_lag.map(x => {
      (col(column) - lag(col(column), x).over(idWindow))
        .alias("diff_%s_lag_%d".format(column, x))
    })
  }

  def features_rolling_mean(columns: Seq[String], window_sizes: Seq[Int], days_lag: Seq[Int]): Seq[Column] = {

    var trfs: Seq[Column] = Seq()

    for (c <- columns) {
      for (w_size <- window_sizes) {
        for (d_lag <- days_lag) {
          val new_name = "%s__lag_%d__wsize_%d__mean".format(c, d_lag, w_size)

          trfs = trfs :+ mean(col(c))
            .over(RollingWindow(w_size, d_lag))
            .alias(new_name)
        }
      }
    }

    trfs
  }

  def features_ewm(columns: Seq[String], alphas: Seq[Double], days_lag: Seq[Int], row_n_column: String = "row_n"):
  Seq[Column] = {

    var trfs: Seq[Column] = Seq()

    for (c <- columns) {
      for (a <- alphas) {
        for (d <- days_lag) {
          val new_name = "%s__lag_%d__alpha_%.2f__ewm".format(c, d, a).replace(".", "_")

          trfs = trfs :+ Ewm.ewm(
            col("row_n") - d, collect_list(c).over(RollingWindow(lag = d)), lit(a)
          ).alias(new_name)
        }
      }
    }

    trfs
  }


  def featuresGeneration()(df: DataFrame): DataFrame = {

    val columns_original = df.columns.map(x => col(x)).toSeq

    val columns_lags = FeaturesGeneration.features_lags(
      column = "sales", days_lag = Seq(28, 28+7, 28+14))

    val columns_lags_diff = FeaturesGeneration.features_differences_with_lags(
      column = "sell_price", days_lag = Seq(28))

    val columns_lags_rolling_mean = FeaturesGeneration.features_rolling_mean(
      columns = Seq("sales"), window_sizes = Seq(7, 14, 28), days_lag = Seq(28))

    val columns_lags_ewm = FeaturesGeneration.features_ewm(
      columns = Seq("sales"), alphas = Seq(0.1, 0.05), days_lag = Seq(28))

    val columns_all = columns_original ++ columns_lags ++ columns_lags_diff ++
      columns_lags_rolling_mean ++ columns_lags_ewm

    df.select(columns_all: _*)
  }


}
