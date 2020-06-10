package ai.m5

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types._

trait ReadCsv {

  def readCsv(inputSchema: StructType, fileLocation: String, sp: SparkSession) : DataFrame = {

    sp.sqlContext
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .schema(inputSchema)
      .load(fileLocation)
  }

  def readCsv(fileLocation: String, sp: SparkSession) : DataFrame = {

    sp.sqlContext
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load(fileLocation)
  }

}
