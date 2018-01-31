package com.unilever.ohub.spark.merging

import java.sql.Timestamp
import java.util.UUID

import com.unilever.ohub.spark.tsv2parquet.ContactPersonRecord
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.functions._

import scala.io.Source

case class GoldenContactPersonRecord(OHUB_CONTACT_PERSON_ID: String, CONTACT_PERSON: ContactPersonRecord, REF_IDS: Seq[String], COUNTRY_CODE: String)
case class ContactPersonMatchingResult(source_id: String, target_id: String, COUNTRY_CODE: String)
case class ContactPersonIdAndCountry(CONTACT_PERSON_CONCAT_ID: String, COUNTRY_CODE: String)

object ContactPersonMerging extends App {

  if (args.length != 3) {
    println("specify MATCHING_INPUT_FILE OPERATOR_INPUT_FILE OUTPUT_FILE")
    sys.exit(1)
  }

  val matchingInputFile = args(0)
  val contactPersonInputFile = args(1)
  val outputFile = args(2)

  println(s"Merging contact persons from [$matchingInputFile] and [$contactPersonInputFile] to [$outputFile]")

  val spark = SparkSession
    .builder()
    .appName(this.getClass.getSimpleName)
    .getOrCreate()

  import spark.implicits._

  val startOfJob = System.currentTimeMillis()

  val contactPersons: Dataset[ContactPersonRecord] = spark.read.parquet(contactPersonInputFile).as[ContactPersonRecord]

  val matches = spark.read.parquet(matchingInputFile)
    .select("source_id", "target_id", "COUNTRY_CODE")
    .as[ContactPersonMatchingResult]

  val groupedContactPersons: Dataset[Seq[ContactPersonRecord]] = matches
    .joinWith(contactPersons, matches("COUNTRY_CODE") === contactPersons("COUNTRY_CODE") and $"target_id" === $"CONTACT_PERSON_CONCAT_ID")
    .groupByKey(_._1.source_id)
    .agg(collect_list("_2").alias("contactPersons").as[Seq[ContactPersonRecord]])
    .joinWith(contactPersons, $"value" === $"CONTACT_PERSON_CONCAT_ID")
    .map(x => x._2 +: x._1._2)

  val matchedIds: Dataset[ContactPersonIdAndCountry] = groupedContactPersons
    .flatMap(_.map(x => ContactPersonIdAndCountry(x.CONTACT_PERSON_CONCAT_ID, x.COUNTRY_CODE.get)))
    .distinct()

  val singletonContactPersons: Dataset[Seq[ContactPersonRecord]] = contactPersons
    .join(matchedIds, Seq("CONTACT_PERSON_CONCAT_ID"), "leftanti")
    .as[ContactPersonRecord]
    .map(Seq(_))

  lazy val sourcePreference = {
    val filename = "/source_preference.tsv"
    val lines = Source.fromInputStream(getClass.getResourceAsStream(filename)).getLines().toSeq
    lines.filterNot(line => line.isEmpty || line.equals("SOURCE\tPRIORITY")).map(_.split("\t")).map(x => (x(0), x(1).toInt)).toMap
  }

  def pickGoldenRecordAndGroupId(contactPersons: Seq[ContactPersonRecord]): GoldenContactPersonRecord = {
    val refIds = contactPersons.map(_.CONTACT_PERSON_CONCAT_ID)
    val goldenRecord = contactPersons.reduce((o1, o2) => {
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
    GoldenContactPersonRecord(id, goldenRecord, refIds, goldenRecord.COUNTRY_CODE.get)
  }

  val goldenRecords: Dataset[GoldenContactPersonRecord] = groupedContactPersons
    .union(singletonContactPersons)
    .map(pickGoldenRecordAndGroupId(_))

  goldenRecords.repartition(60).write.mode(Overwrite).partitionBy("COUNTRY_CODE").format("parquet").save(outputFile)

  println(s"Went from ${contactPersons.count} to ${spark.read.parquet(outputFile).count} records")
  println(s"Done in ${(System.currentTimeMillis - startOfJob) / 1000}s")
}
