package ai.m5

import org.apache.spark.sql.SparkSession
import org.apache.spark.serializer.KryoSerializer

trait SparkSessionTestWrapper {

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("mt time series example")
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .getOrCreate()
  }

}
