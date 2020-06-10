package ai.m5

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object Ewm {

  val ewm: UserDefinedFunction = udf((rowNumber: Int, tOrderedValues: Seq[Double], alpha: Double) => {

    val adjustedWeights = (0 until rowNumber).foldLeft(new Array[Double](rowNumber)) {
      (accumulator, index) =>
        accumulator(index) = scala.math.pow(1 - alpha, rowNumber - index)
        accumulator
    }

    (adjustedWeights, tOrderedValues.slice(0, rowNumber + 1)).zipped.map(_ * _).sum / adjustedWeights.sum
  })

}
