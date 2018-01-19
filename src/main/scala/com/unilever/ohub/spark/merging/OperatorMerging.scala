package com.unilever.ohub.spark.merging

import java.sql.Timestamp
import java.util.UUID

import com.unilever.ohub.spark.tsv2parquet.OperatorRecord
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.functions._

import scala.io.Source

case class OperatorWrapper(id: String, operator: OperatorRecord)

case class GoldenOperatorRecord(OHUB_OPERATOR_ID: String, OPERATOR: OperatorRecord, REF_IDS: Seq[String])

case class MatchingResult(source_id: String, target_id: String, COUNTRY_CODE: String)

case class IdAndCountry(OPERATOR_CONCAT_ID: String, COUNTRY_CODE: String)

object OperatorMerging extends App {

  if (args.length != 3) {
    println("specify MATCHING_INPUT_FILE OPERATOR_INPUT_FILE OUTPUT_FILE")
    sys.exit(1)
  }

  val matchingInputFile = args(0)
  val operatorInputFile = args(1)
  val outputFile = args(2)

  println(s"Merging operators from [$matchingInputFile] and [$operatorInputFile] to [$outputFile]")

  val spark = SparkSession.
    builder().
    appName(this.getClass.getSimpleName).
    getOrCreate()

  import spark.implicits._

  val startOfJob = System.currentTimeMillis()

  val operators: Dataset[OperatorRecord] = spark.read.parquet(operatorInputFile).as[OperatorRecord]

  //TODO Do something smart with lit("DK")
  val matches = spark.read.parquet(matchingInputFile).select("source_id", "target_id")
    .withColumn("COUNTRY_CODE", lit("DK"))
    .as[MatchingResult]

  val groupedOperators: Dataset[Seq[OperatorRecord]] = matches
    .joinWith(operators, matches("COUNTRY_CODE") === operators("COUNTRY_CODE") and $"target_id" === $"OPERATOR_CONCAT_ID")
    .groupByKey(_._1.source_id)
    .agg(collect_list("_2").alias("operators").as[Seq[OperatorRecord]])
    .joinWith(operators, $"value" === $"OPERATOR_CONCAT_ID")
    .map(x => x._2 +: x._1._2)

  val matchedIds: Dataset[IdAndCountry] = groupedOperators
    .flatMap(_.map(x => IdAndCountry(x.OPERATOR_CONCAT_ID, x.COUNTRY_CODE.get)))
    .distinct()

  val singletonOperators: Dataset[Seq[OperatorRecord]] = operators
//    .filter($"COUNTRY_CODE" === "DK")
    .join(matchedIds, Seq("OPERATOR_CONCAT_ID", "COUNTRY_CODE"), "leftanti")
    .as[OperatorRecord]
    .map(Seq(_))

  lazy val sourcePreference = {
    val filename = "/source_preference.tsv"
    val lines = Source.fromInputStream(getClass.getResourceAsStream(filename)).getLines().toSeq
    lines.filterNot(line => line.isEmpty || line.equals("SOURCE\tPRIORITY")).map(_.split("\t")).map(x => (x(0), x(1).toInt)).toMap
  }

  def pickGoldenRecordAndGroupId(operators: Seq[OperatorRecord]): GoldenOperatorRecord = {
    val refIds = operators.map(_.OPERATOR_CONCAT_ID)
    val goldenRecord = operators.reduce((o1, o2) => {
      val preference1 = sourcePreference.get(o1.SOURCE.getOrElse("UNKNOWN")).getOrElse(Int.MaxValue)
      val preference2 = sourcePreference.get(o2.SOURCE.getOrElse("UNKNOWN")).getOrElse(Int.MaxValue)
      if (preference1 < preference2) o1
      else if (preference1 > preference2) o2
      else { // same source preference
        val created1 = o1.DATE_CREATED.getOrElse(new Timestamp(System.currentTimeMillis))
        val created2 = o1.DATE_CREATED.getOrElse(new Timestamp(System.currentTimeMillis))
        if (created1.before(created2)) o1 else o2
      }
    })
    val id = UUID.randomUUID().toString
    GoldenOperatorRecord(id, goldenRecord, refIds)
  }

  val goldenRecords: Dataset[GoldenOperatorRecord] = groupedOperators
    .union(singletonOperators)
    .map(pickGoldenRecordAndGroupId(_))

  goldenRecords.repartition(60).write.mode(Overwrite).format("parquet").save(outputFile)

    println(s"Went from ${operators.count} to ${spark.read.parquet(outputFile).count} records")
  println(s"Done in ${(System.currentTimeMillis - startOfJob) / 1000}s")
}
