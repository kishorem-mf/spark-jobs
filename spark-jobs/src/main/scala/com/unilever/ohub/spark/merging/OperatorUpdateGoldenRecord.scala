package com.unilever.ohub.spark.merging

import java.util

import com.unilever.ohub.spark.{ DefaultConfig, SparkJobWithDefaultDbConfig }
import com.unilever.ohub.spark.domain.entity.Operator
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.DomainDataProvider
import org.apache.spark.sql.{ Dataset, SparkSession }
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object OperatorUpdateGoldenRecord extends SparkJobWithDefaultDbConfig with GoldenRecordPicking[Operator] {

  def markGoldenRecord(sourcePreference: Map[String, Int])(operators: Seq[Operator]): Seq[Operator] = {
    val goldenRecord = pickGoldenRecord(sourcePreference, operators)
    operators.map(o ⇒ o.copy(isGoldenRecord = o == goldenRecord))
  }

  def transform(
    spark: SparkSession,
    operators: Dataset[Operator],
    sourcePreference: Map[String, Int],
    sizeThreshold: Int = 250000
  ): Dataset[Operator] = {
    import spark.implicits._

    val toBigMan: Array[String] = operators
      .groupBy("ohubId")
      .count
      .filter($"count" >= sizeThreshold)
      .select("ohubId")
      .as[String]
      .collect()

    val goldenRecordCorrect = operators
      .map(x ⇒ x.ohubId.get -> x)
      .toDF("ohubId", "operator")
      .filter($"ohubId".isin(toBigMan: _*) === false)
      .groupBy("ohubId")
      .agg(collect_list($"operator").as("operators"))
      .select("operators")
      .as[Seq[Operator]]
      .flatMap(markGoldenRecord(sourcePreference))
      .checkpoint

    val w = Window.partitionBy($"ohubId").orderBy($"concatId")
    val goldenRecordCheap = operators
      .filter($"ohubId".isin(toBigMan: _*) === true)
      .withColumn("rn", row_number().over(w))
      .withColumn("isGoldenRecord", when($"rn" === 1, true).otherwise(false))
      .drop("rn")
      .as[Operator]
      .checkpoint

    goldenRecordCorrect.unionByName(goldenRecordCheap)
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
