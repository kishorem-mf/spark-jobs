package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.domain.entity.Operator
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.tsv2parquet.DomainDataProvider
import org.apache.spark.sql.{ Dataset, SparkSession }
import org.apache.spark.sql.functions._

object OperatorUpdateGoldenRecord extends SparkJob with GoldenRecordPicking[Operator] {

  case class oHubIdAndRecord(ohubId: String, operator: Operator)

  def markGoldenRecord(sourcePreference: Map[String, Int])(operators: Seq[Operator]): Seq[Operator] = {
    val goldenRecord = pickGoldenRecord(sourcePreference, operators)
    operators.map(o ⇒ o.copy(isGoldenRecord = (o == goldenRecord)))
  }

  def transform(
    spark: SparkSession,
    operators: Dataset[Operator],
    sourcePreference: Map[String, Int]
  ): Dataset[Operator] = {
    import spark.implicits._

    operators
      .map(x ⇒ oHubIdAndRecord(x.ohubId.get, x))
      .groupByKey(_.ohubId)
      .agg(collect_list("operator").as("operator").as[Seq[Operator]])
      .map(_._2)
      .flatMap(markGoldenRecord(sourcePreference))
      .repartition(60)
  }

  override val neededFilePaths = Array("OPERATOR_INPUT_FILE", "OUTPUT_FILE")

  override def run(spark: SparkSession, filePaths: Product, storage: Storage): Unit = {
    run(spark, filePaths, storage, DomainDataProvider(spark, storage))
  }

  protected[merging] def run(spark: SparkSession, filePaths: Product, storage: Storage, dataProvider: DomainDataProvider): Unit = {
    import spark.implicits._

    val (operatorInputFile: String, outputFile: String) = filePaths

    val operators = storage
      .readFromParquet[Operator](operatorInputFile)

    val transformed = transform(spark, operators, dataProvider.sourcePreferences)

    storage
      .writeToParquet(transformed, outputFile, partitionBy = Seq("countryCode"))
  }
}
