package com.unilever.ohub.spark.merging

import java.sql.Timestamp
import java.util.UUID

import com.unilever.ohub.spark.tsv2parquet.OperatorRecord
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.functions.collect_list

import scala.io.Source

case class OperatorWrapper(id:String, operator:OperatorRecord)
case class GoldenOperatorRecord(OHUB_OPERATOR_ID:String, OPERATOR:OperatorRecord, REF_IDS: Seq[String])

object OperatorMerging extends App {

  if (args.length != 3) {
    println("specify MATCHING_INPUT_FILE OPERATOR_INPUT_FILE OUTPUT_FILE")
    sys.exit(1)
  }

  val matchingInputFile = args(0)
  val operatorInputFile = args(1)
  val outputFile = args(2)

  println(s"Merging operators from [$matchingInputFile] and [$operatorInputFile] to [$outputFile]")

  val spark = SparkSession
    .builder()
    .appName(this.getClass.getSimpleName)
    .getOrCreate()

  import spark.implicits._

  val startOfJob = System.currentTimeMillis()

  val operators:Dataset[OperatorWrapper] = spark.read.parquet(operatorInputFile)
    .as[OperatorRecord]
    .filter($"COUNTRY_CODE" === "DK")
//    .select($"OPERATOR_CONCAT_ID".alias("id"), $"_".alias("operator"))
//    .as[OperatorWrapper]
    .map(o => OperatorWrapper(id = o.OPERATOR_CONCAT_ID, operator = o))


  operators.printSchema()
  operators.show()
  System.exit(0)

  val matches = spark.read.parquet(matchingInputFile).select("source_id", "target_id")

  val groupedOperators:Dataset[Seq[OperatorRecord]] = matches
    .joinWith(operators, $"id" === $"target_id")
    .select("source_id", "operator")
    .groupBy("source_id")
    .agg(collect_list("operator").alias("operators"))
    .join(operators.select($"id".alias("source_id"), $"operator"), "source_id")
    .select($"operator".as[OperatorRecord], $"operators".as[Seq[OperatorRecord]])
    .map(x => x._1 +: x._2)

  val matchedIds = groupedOperators
    .flatMap(_.map(_.OPERATOR_CONCAT_ID))
    .distinct()
    .select($"value".alias("id"))

  val singletonOperators:Dataset[Seq[OperatorRecord]] = operators
    .join(matchedIds, Seq("id"), "leftanti")
    .select($"operator".as[OperatorRecord])
    .map(Seq(_))

  lazy val sourcePreference = {
    val filename = "/source_preference.tsv"
    val lines = Source.fromInputStream(getClass.getResourceAsStream(filename)).getLines().toSeq
    lines.filterNot(line => line.isEmpty || line.equals("SOURCE\tPRIORITY")).map(_.split("\t")).map(x => (x(0), x(1).toInt)).toMap
  }

  def pickGoldenRecordAndGroupId(operators: Seq[OperatorRecord]):GoldenOperatorRecord = {
    val refIds = operators.map(o => s"${o.COUNTRY_CODE.get}~${o.SOURCE.get}~${o.REF_OPERATOR_ID.get}")
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

  val goldenRecords:Dataset[GoldenOperatorRecord] = groupedOperators
    .union(singletonOperators)
    .map(pickGoldenRecordAndGroupId(_))

  goldenRecords.repartition(60).write.mode(Overwrite).format("parquet").save(outputFile)

//  println(s"Went from ${operators.count} to ${spark.read.parquet(outputFile).count} records")
  println(s"Done in ${(System.currentTimeMillis - startOfJob) / 1000}s")
}
