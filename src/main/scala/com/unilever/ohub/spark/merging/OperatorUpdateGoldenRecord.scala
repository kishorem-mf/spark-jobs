package com.unilever.ohub.spark.merging

import java.sql.Timestamp
import java.util.UUID

import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.domain.entity.Operator
import com.unilever.ohub.spark.sql.JoinType
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.collect_list

object OperatorUpdateGoldenRecord extends SparkJob with OperatorGoldenRecord {


  def markGoldenRecord(sourcePreference: Map[String, Int])
                      (operators: Seq[Operator]): Seq[Operator] = {

    val goldenRecord = pickGoldenRecord(sourcePreference, operators)
    operators.map(o => o.copy(isGoldenRecord = (o == goldenRecord)))
  }

  def transform(
                 spark: SparkSession,
                 operators: Dataset[Operator],
                 sourcePreference: Map[String, Int]
               ): Dataset[Operator] = {
    import spark.implicits._

    operators
      .groupByKey(_.ohubId.get)
      .agg(collect_list("operator").alias("operators").as[Seq[Operator]])
      .map(_._2)
      .flatMap(markGoldenRecord(sourcePreference))
      .repartition(60)
  }


  override val neededFilePaths = Array("MATCHING_INPUT_FILE", "OPERATOR_INPUT_FILE", "OUTPUT_FILE")

  override def run(spark: SparkSession, filePaths: Product, storage: Storage): Unit = {
    import spark.implicits._

    val (matchingInputFile: String, operatorInputFile: String, outputFile: String) = filePaths

    log.info(s"Merging operators from [$matchingInputFile] and [$operatorInputFile] to [$outputFile]")

    val operators = storage
      .readFromParquet[Operator](operatorInputFile)

    val transformed = transform(spark, operators, storage.sourcePreference)

    storage
      .writeToParquet(transformed, outputFile, partitionBy = Seq("countryCode"))
  }
}
