package ai.m5.Features

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object Ewm {

  val ewm: UserDefinedFunction = udf((rowNumber: Int,
                                      tOrderedValues: Seq[Double],
                                      alpha: Double) => {

    val reluRowNumber = scala.math.max(rowNumber, 0)

    val adjustedWeights = (0 until reluRowNumber).foldLeft(new Array[Double](reluRowNumber)) {
      (accumulator, index) =>
        accumulator(index) = scala.math.pow(1 - alpha, rowNumber - index)
        accumulator
    }

    (adjustedWeights, tOrderedValues.slice(0, reluRowNumber))
      .zipped.map(_ * _).sum / adjustedWeights.sum

  })

}
