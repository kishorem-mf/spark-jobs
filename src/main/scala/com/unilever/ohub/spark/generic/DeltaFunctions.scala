package com.unilever.ohub.spark.generic
import org.apache.spark.sql.{ Dataset, SparkSession }
import scala.reflect.runtime.universe.TypeTag
import com.unilever.ohub.spark.sql.JoinType

object DeltaFunctions {

  def integrate[T <: Product: TypeTag](
    spark: SparkSession,
    current: Dataset[T],
    previous: Dataset[T],
    key: String
  ): Dataset[T] = {
    import spark.implicits._
    val newIntegrated = current
      .join(previous, Seq(key), JoinType.LeftAnti)
      .as[T]

    val updated = newIntegrated
      .joinWith(
        previous,
        newIntegrated(key) === previous(key),
        JoinType.Inner)
      .map {
        case (left, right) ⇒
          (left, left != right)
      }
      .filter(_._2)
      .map(_._1)

    previous.union(updated)
  }

}
