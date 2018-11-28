package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.{ DefaultConfig, SparkJobWithDefaultDbConfig }
import com.unilever.ohub.spark.domain.entity.Operator
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.DomainDataProvider
import org.apache.spark.sql.{ Dataset, SparkSession }
import org.apache.spark.sql.functions._

object OperatorUpdateGoldenRecord extends SparkJobWithDefaultDbConfig with GoldenRecordPicking[Operator] {

  def markGoldenRecord(sourcePreference: Map[String, Int])(operators: Seq[Operator]): Seq[Operator] = {
    val goldenRecord = pickGoldenRecord(sourcePreference, operators)
    operators.map(o ⇒ o.copy(isGoldenRecord = o == goldenRecord))
  }

  def transform(
    spark: SparkSession,
    operators: Dataset[Operator],
    sourcePreference: Map[String, Int]
  ): Dataset[Operator] = {
    import spark.implicits._

    operators
      .map(x ⇒ x.ohubId.get -> x)
      .toDF("ohubId", "operator")
      .groupBy("ohubId")
      .agg(collect_list($"operator").as("operators"))
      .as[(String, Seq[Operator])]
      .map(_._2)
      .flatMap(markGoldenRecord(sourcePreference))
  }

  override def run(spark: SparkSession, config: DefaultConfig, storage: Storage): Unit = {
    run(spark, config, storage, DomainDataProvider(spark))
  }

  protected[merging] def run(spark: SparkSession, config: DefaultConfig, storage: Storage, dataProvider: DomainDataProvider): Unit = {
    val operators = storage.readFromParquet[Operator](config.inputFile)
    val transformed = transform(spark, operators, dataProvider.sourcePreferences)

    storage.writeToParquet(transformed, config.outputFile)
  }
}
