package com.unilever.ohub.spark.deduplicate

import java.sql.Timestamp

import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.domain.entity.Operator
import com.unilever.ohub.spark.sql.JoinType
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{Dataset, SparkSession}

object OperatorDeduplication extends SparkJob {

  def transform(
                 spark: SparkSession,
                 integratedOperators: Dataset[Operator],
                 newOperators: Dataset[Operator]
               ): (Dataset[Operator], Dataset[Operator]) = {
    import spark.implicits._

    val dailyNew = newOperators
      .join(integratedOperators, Seq("concatId"), JoinType.LeftAnti)
      .as[Operator]

    val dailyDupe = newOperators
      .join(dailyNew, Seq("concatId"), JoinType.LeftAnti)
      .as[Operator]

    val deduped = integratedOperators
      .union(dailyDupe)
      .groupByKey(_.concatId)
      .reduceGroups((left, right) => {
        val bothTimestampsAvailable = left.dateUpdated.isDefined && right.dateUpdated.isDefined
        def operatorWithLatestUpdate(leftOperator: Operator, rightOperator: Operator) = {
          val leftTime = leftOperator.dateUpdated.get
          val rightTime = rightOperator.dateUpdated.get

          if (leftTime.after(rightTime)) leftOperator else rightOperator
        }

        if (bothTimestampsAvailable) operatorWithLatestUpdate(left, right) else right
      })
      .map(_._2)

    (deduped, dailyNew)
  }

  override val neededFilePaths = Array("INTEGRATED_INPUT", "DAILY_INPUT", "INTEGRATED_OUTPUT", "DAILY_NEW_OUTPUT")

  override def run(spark: SparkSession, filePaths: Product, storage: Storage): Unit = {
    import spark.implicits._

    val (
      integrated: String,
      daily: String,
      ingeratedOutput: String,
      dailyNewOutput: String) = filePaths

    val integratedOperators = storage.readFromParquet[Operator](integrated)
    val dailyOperators = storage.readFromParquet[Operator](daily)

    val (updatedOperators, newOperators) = transform(spark, integratedOperators, dailyOperators)

    storage
      .writeToParquet(updatedOperators, ingeratedOutput, partitionBy = Seq("countryCode"))
    storage
      .writeToParquet(newOperators, dailyNewOutput, partitionBy = Seq("countryCode"))
  }
}
