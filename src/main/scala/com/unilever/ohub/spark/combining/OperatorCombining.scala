package com.unilever.ohub.spark.combining

import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.domain.entity.Operator
import com.unilever.ohub.spark.sql.JoinType
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{Dataset, SparkSession}

object OperatorCombining extends SparkJob {

  def transform(
                 spark: SparkSession,
                 integratedMatched: Dataset[Operator],
                 newGoldenRecords: Dataset[Operator]
               ): Dataset[Operator] = {
    import spark.implicits._

  }

  override val neededFilePaths = Array("INTEGRATED_MATCHED", "NEW_GOLDEN", "NEW_INTEGRATED_OUTPUT")

  override def run(spark: SparkSession, filePaths: Product, storage: Storage): Unit = {
    import spark.implicits._

    val (
      integratedMatchedFn: String,
      newGoldenFn: String,
      newIntegratedFn: String) = filePaths

    val integratedMatched = storage.readFromParquet[Operator](integratedMatchedFn)
    val newGolden = storage.readFromParquet[Operator](newGoldenFn)

    val newIntegrated = transform(spark, integratedMatched, newGolden)

    storage
      .writeToParquet(newIntegrated, newIntegratedFn, partitionBy = Seq("countryCode"))
  }
}
