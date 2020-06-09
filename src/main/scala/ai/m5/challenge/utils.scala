package ai.m5.challenge

import org.apache.spark.sql.DataFrame

object utils {

  def limitRows(nrows: Int)(df: DataFrame): DataFrame = {

    if (nrows > 0) {
      df.limit(nrows)
    } else {
      df
    }

  }

}
