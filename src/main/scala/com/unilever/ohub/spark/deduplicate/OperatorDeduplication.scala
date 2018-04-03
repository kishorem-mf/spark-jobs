package com.unilever.ohub.spark.deduplicate

import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.domain.entity.Operator
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
      .joinWith(integratedOperators, newOperators("concatId") === integratedOperators("concatId"), "left_anti")
      .filter(_._2 == null)
      .map(_._1)

    val dailyDupe = newOperators
      .joinWith(dailyNew, newOperators("concatId") === dailyNew("concatId"), "left_anti")
      .filter(_._2 == null)
      .map(_._1)

    val deduped = integratedOperators
      .union(dailyDupe)
      .groupByKey(_.concatId)
      .reduceGroups((a, b) => {
        if (a.dateUpdated.isDefined && b.dateUpdated.isDefined) {
          if (a.dateUpdated.get.after(b.dateUpdated.get)) a else b
        }
        else {
          a
        }
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
