package com.unilever.ohub.spark.merging

import java.sql.Timestamp
import java.util.UUID

import com.unilever.ohub.spark.generic.FileSystems
import com.unilever.ohub.spark.tsv2parquet.OperatorRecord
import org.apache.log4j.{ LogManager, Logger }
import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ Dataset, SparkSession }

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

object OperatorMerging extends App {
  implicit private val log: Logger = LogManager.getLogger(this.getClass)

  val (matchingInputFile: String, operatorInputFile: String, outputFile: String) = FileSystems.getFileNames(
    args,
    "MATCHING_INPUT_FILE", "OPERATOR_INPUT_FILE", "OUTPUT_FILE"
  )

  log.info(s"Merging operators from [$matchingInputFile] and [$operatorInputFile] to [$outputFile]")

  val spark = SparkSession
    .builder()
    .appName(this.getClass.getSimpleName)
    .getOrCreate()

  import spark.implicits._

  val startOfJob = System.currentTimeMillis()

  val operators = spark.read.parquet(operatorInputFile).as[OperatorRecord]

  val matches = spark.read.parquet(matchingInputFile).select("source_id", "target_id","COUNTRY_CODE")
    .as[MatchingResult]

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
    .join(matchedIds, Seq("OPERATOR_CONCAT_ID"), "leftanti")
    .as[OperatorRecord]
    .map(Seq(_))

  lazy val sourcePreference = {
    val filename = "/source_preference.tsv"
    val lines = Source.fromInputStream(getClass.getResourceAsStream(filename)).getLines().toSeq
    lines
      .filter(_.nonEmpty)
      .filterNot(_.equals("SOURCE\tPRIORITY"))
      .map(_.split("\t"))
      .map(lineParts => lineParts(0) -> lineParts(1).toInt)
      .toMap
  }

  def pickGoldenRecordAndGroupId(operators: Seq[OperatorRecord]): GoldenOperatorRecord = {
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

  val goldenRecords: Dataset[GoldenOperatorRecord] = groupedOperators
    .union(singletonOperators)
    .map(pickGoldenRecordAndGroupId)

  goldenRecords
    // TODO remove select line
    .select("OHUB_OPERATOR_ID", "OPERATOR.*")
    .repartition(60)
    .write
    .mode(Overwrite)
    .partitionBy("COUNTRY_CODE")
    .format("parquet")
    .save(outputFile)

  log.info(s"Went from ${operators.count} to ${spark.read.parquet(outputFile).count} records")
  log.info(s"Done in ${(System.currentTimeMillis - startOfJob) / 1000}s")
}
