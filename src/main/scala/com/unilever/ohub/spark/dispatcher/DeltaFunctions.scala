package com.unilever.ohub.spark.dispatcher

import com.unilever.ohub.spark.sql.JoinType
import org.apache.spark.sql.{ Dataset, SparkSession }

import scala.reflect.runtime.universe.TypeTag

object DeltaFunctions extends DeltaFunctions

trait DeltaFunctions {

  // integrate a delta into an existing dataset, upserting by a given key
  def integrate[T <: Product: TypeTag](
    spark: SparkSession,
    delta: Dataset[T],
    previous: Dataset[T],
    key: String
  ): Dataset[T] = {
    import spark.implicits._

    val newIntegrated = delta
      .join(previous, Seq(key), JoinType.LeftAnti)
      .as[T]

    val updated = delta
      .joinWith(
        previous,
        delta(key) === previous(key),
        JoinType.Inner)
      .map {
        case (left, right) ⇒ (left, left != right)
      }
      .filter(_._2)
      .map {
        case (result, _) ⇒ result
      }

    newIntegrated.unionByName(updated)
  }
}
