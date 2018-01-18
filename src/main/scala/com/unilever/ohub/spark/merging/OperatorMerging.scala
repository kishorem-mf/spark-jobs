package com.unilever.ohub.spark.merging

import java.sql.Timestamp
import java.util.UUID

import com.unilever.ohub.spark.tsv2parquet.OperatorRecord
import org.apache.spark.sql.{Dataset, SparkSession}
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

  val operators:Dataset[OperatorWrapper] = spark.read.parquet(operatorInputFile)
    .filter("COUNTRY_CODE = 'DK'")
    .as[OperatorRecord]
    .map(o => OperatorWrapper(id = s"${o.COUNTRY_CODE.get}~${o.SOURCE.get}~${o.REF_OPERATOR_ID.get}", operator = o))

  val matches = spark.read.parquet(matchingInputFile).select("source_id", "target_id")

  // join the full operators to the target_id side of the matches
  val targetIdJoined = matches.join(operators.select($"id".alias("target_id"), $"operator"), "target_id")
    .select("source_id", "operator")

  // group up the operators based on the source id
  val groupedBySource = targetIdJoined.groupBy("source_id").agg(collect_list("operator").alias("operators"))

  // fetch the operator that goes with the source id and stick it in the group with the operators from the groupBy
  val fullyGroupedOperators:Dataset[Seq[OperatorRecord]] = groupedBySource.join(operators.select($"id".alias("source_id"), $"operator"), "source_id")
    .select($"operator".as[OperatorRecord], $"operators".as[Seq[OperatorRecord]])
    .map(x => x._1 +: x._2)

  // all the IDs of everything from the matching, but we also need individual records that didn't match anything ...
  val matchedIds = fullyGroupedOperators
    .flatMap(_.map(o => s"${o.COUNTRY_CODE.get}~${o.SOURCE.get}~${o.REF_OPERATOR_ID.get}"))
    .createOrReplaceTempView("MATCHED_IDS")

  spark.sql("select * from MATCHED_IDS").show()

  // but we also need all the operators that didn't match any others
  // TODO implement me and do a union

  println("Temp Done")
  System.exit(0)

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
      else { // same preference
        val created1 = o1.DATE_CREATED.getOrElse(new Timestamp(System.currentTimeMillis))
        val created2 = o1.DATE_CREATED.getOrElse(new Timestamp(System.currentTimeMillis))
        if (created1.before(created2)) o1 else o2
      }
    })
    val id = UUID.randomUUID().toString
    GoldenOperatorRecord(id, goldenRecord, refIds)
  }

  val goldenRecords = fullyGroupedOperators.map(pickGoldenRecordAndGroupId(_)).select("OHUB_OPERATOR_ID", "REF_IDS", "OPERATOR")

//  fullyGroupedOperators.write.mode(Overwrite).format("parquet").save(outputFile)

  println("Done")


}
