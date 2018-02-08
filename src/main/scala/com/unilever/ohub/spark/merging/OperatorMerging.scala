package com.unilever.ohub.spark.merging

import java.sql.Timestamp
import java.util.UUID

import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.sql.LeftAnti
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.tsv2parquet.OperatorRecord
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ Dataset, Row, SparkSession }

import scala.io.Source

case class GoldenOperatorRecord(
  OHUB_OPERATOR_ID: String,
  OPERATOR: OperatorRecord,
  REF_IDS: Seq[String],
  COUNTRY_CODE: String
)

case class MatchingResult(source_id: String, target_id: String, COUNTRY_CODE: String)

case class IdAndCountry(OPERATOR_CONCAT_ID: String, COUNTRY_CODE: String)

case class MatchingResultAndOperator(
  matchingResult: MatchingResult,
  operator: OperatorRecord
) {
  val sourceId: String = matchingResult.source_id
}

object OperatorMerging extends SparkJob {
  def pickGoldenRecordAndGroupId(sourcePreference: Map[String, Int])
                                (operators: Seq[OperatorRecord]): GoldenOperatorRecord = {
    val refIds = operators.map(_.OPERATOR_CONCAT_ID)
    val goldenRecord = operators.reduce((o1, o2) => {
      val preference1 = sourcePreference.getOrElse(o1.SOURCE.getOrElse("UNKNOWN"), Int.MaxValue)
      val preference2 = sourcePreference.getOrElse(o2.SOURCE.getOrElse("UNKNOWN"), Int.MaxValue)
      if (preference1 < preference2) o1
      else if (preference1 > preference2) o2
      else { // same source preference
        val created1 = o1.DATE_CREATED.getOrElse(new Timestamp(System.currentTimeMillis))
        val created2 = o1.DATE_CREATED.getOrElse(new Timestamp(System.currentTimeMillis))
        if (created1.before(created2)) o1 else o2
      }
    })
    val id = UUID.randomUUID().toString
    GoldenOperatorRecord(id, goldenRecord, refIds, goldenRecord.COUNTRY_CODE.get)
  }

  def transform(
    spark: SparkSession,
    operators: Dataset[OperatorRecord],
    matches: Dataset[MatchingResult],
    sourcePreference: Map[String, Int]
  ): Dataset[Row] = {
    import spark.implicits._

    val groupedOperators = matches
      .joinWith(
        operators,
        matches("COUNTRY_CODE") === operators("COUNTRY_CODE") and $"target_id" === $"OPERATOR_CONCAT_ID"
      )
      .map((MatchingResultAndOperator.apply _).tupled)
      .groupByKey(_.sourceId)
      .agg(collect_list("operator").alias("operators").as[Seq[OperatorRecord]])
      .joinWith(operators, $"value" === $"OPERATOR_CONCAT_ID")
      .map(x => x._2 +: x._1._2)

    val matchedIds = groupedOperators
      .flatMap(_.map(x => IdAndCountry(x.OPERATOR_CONCAT_ID, x.COUNTRY_CODE.get)))
      .distinct()

    val singletonOperators = operators
      .join(matchedIds, Seq("OPERATOR_CONCAT_ID"), LeftAnti)
      .as[OperatorRecord]
      .map(Seq(_))

    val pickGoldenRecordAndGroupIdFunc = pickGoldenRecordAndGroupId(sourcePreference) _

    groupedOperators
      .union(singletonOperators)
      .map(pickGoldenRecordAndGroupIdFunc)
      // TODO remove select line
      .select("OHUB_OPERATOR_ID", "OPERATOR.*")
      .repartition(60)
  }

  override val neededFilePaths = Array("MATCHING_INPUT_FILE", "OPERATOR_INPUT_FILE", "OUTPUT_FILE")

  override def run(spark: SparkSession, filePaths: Product, storage: Storage): Unit = {
    import spark.implicits._

    val (matchingInputFile: String, operatorInputFile: String, outputFile: String) = filePaths

    log.info(s"Merging operators from [$matchingInputFile] and [$operatorInputFile] to [$outputFile]")

    val operators = storage
      .readFromParquet[OperatorRecord](operatorInputFile)

    val matches = storage
      .readFromParquet[MatchingResult](
        matchingInputFile,
        selectColumns = $"source_id", $"target_id", $"COUNTRY_CODE"
      )

    val transformed = transform(spark, operators, matches, storage.sourcePreference)

    storage
      .writeToParquet(transformed, outputFile, partitionBy = "COUNTRY_CODE")
  }
}
