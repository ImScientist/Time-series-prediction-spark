package ai.m5

import org.apache.spark.sql.functions.{col, lit, udf, array, struct, explode, row_number}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.{Window, WindowSpec}

object PreprocessingMerge {

  implicit class DataFrameFunctions(df: DataFrame) {

    /** Convert [[org.apache.spark.sql.DataFrame]] from wide to long format.
      *
      * melt is (kind of) the inverse of pivot
      * melt is currently (02/2017) not implemented in spark
      *
      * @see reshape packe in R (https://cran.r-project.org/web/packages/reshape/index.html)
      * @see this is a scala adaptation of http://stackoverflow.com/questions/41670103/pandas-melt-function-in-apache-spark
      * @todo method overloading for simple calling
      * @param id_vars    the columns to preserve
      * @param value_vars the columns to melt
      * @param var_name   the name for the column holding the melted columns names
      * @param value_name the name for the column holding the values of the melted columns
      *
      */

    def melt(
              id_vars: Seq[String], value_vars: Seq[String],
              var_name: String = "variable", value_name: String = "value"): DataFrame = {

      // Create array<struct<variable: str, value: ...>>
      val _vars_and_vals = array((for (c <- value_vars) yield {
        struct(lit(c).alias(var_name), col(c).alias(value_name))
      }): _*)

      // Add to the DataFrame and explode
      val _tmp = df.withColumn("_vars_and_vals", explode(_vars_and_vals))

      val cols = id_vars.map(col _) ++ {
        for (x <- List(var_name, value_name)) yield {
          col("_vars_and_vals")(x).alias(x)
        }
      }

      _tmp.select(cols: _*)

    }
  }


  def merge_data(spark: SparkSession, data_dir: String, nrows: Int = -1): DataFrame = {

    val calendar = spark.read.parquet(data_dir + "/trf/calendar.parquet")
    val prices = spark.read.parquet(data_dir + "/trf/prices.parquet")
    val sales = spark.read.parquet(data_dir + "/trf/sales.parquet")
      .transform(Utils.limitRows(nrows = nrows))

    val id_vars = Seq("id_indexed", "item_id_indexed", "dept_id_indexed",
      "cat_id_indexed", "store_id_indexed", "state_id_indexed")

    val value_vars = sales.columns.filter(_.slice(0, 2) == "d_").toSeq

    val getDay = udf((a: String) => a.substring(2), StringType)

    val idWindow: WindowSpec = Window.partitionBy(col("id_indexed")).orderBy(col("d"))

    sales
      .melt(
        id_vars = id_vars,
        value_vars = value_vars,
        var_name = "d",
        value_name = "sales"
      )
      .withColumn("d", getDay(col("d")).cast(ShortType))
      .join(calendar, Seq("d"), "left")
      //.join(prices, Seq("store_id_indexed", "item_id_indexed", "wm_yr_wk"), "left")
      .withColumn("sell_price", lit(9.58))
      .withColumn("row_n", row_number().over(idWindow))
      .withColumn("sales", col("sales").cast(DoubleType))
  }

}
