package ai.m5

import org.apache.spark.sql.DataFrame

object Utils {

  def limitRows(nrows: Int)(df: DataFrame): DataFrame = {

    if (nrows > 0) {
      df.limit(nrows)
    } else {
      df
    }

  }

}
