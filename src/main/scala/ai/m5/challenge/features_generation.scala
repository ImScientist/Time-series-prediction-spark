package ai.m5.challenge

import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions.{col, lit, mean, collect_list, lag}

object features_generation {

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

  def features_lags(column: String, lags: Seq[Int]): Seq[Column] = {

    lags.map(x => {
      lag(col(column), x).over(idWindow).alias("%s_lag_%d".format(column, x))
    })
  }

  def features_differences_with_lags(column: String, lags: Seq[Int]): Seq[Column] = {

    lags.map(x => {
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

  def features_ewm(columns: Seq[String], alphas: Seq[Double], days_shift: Seq[Int], row_n_column: String = "row_n"):
  Seq[Column] = {

    var trfs: Seq[Column] = Seq()

    for (c <- columns) {
      for (a <- alphas) {
        for (d <- days_shift) {
          val new_name = "%s__lag_%d__alpha_%.2f__ewm".format(c, d, a).replace(".", "_")

          trfs = trfs :+ ewm.ewm(
            col("row_n") - d, collect_list(c).over(RollingWindow(lag = d)), lit(a)
          ).alias(new_name)
        }
      }
    }

    trfs
  }


}
