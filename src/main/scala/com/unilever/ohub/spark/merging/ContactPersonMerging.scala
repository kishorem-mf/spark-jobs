package com.unilever.ohub.spark.merging

import java.sql.Timestamp
import java.util.UUID

import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.tsv2parquet.ContactPersonRecord
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ Dataset, SparkSession }

import scala.io.Source

case class GoldenContactPersonRecord(
  OHUB_CONTACT_PERSON_ID: String,
  CONTACT_PERSON: ContactPersonRecord,
  REF_IDS: Seq[String],
  COUNTRY_CODE: String
)

case class ContactPersonMatchingResult(source_id: String, target_id: String, COUNTRY_CODE: String)

case class ContactPersonIdAndCountry(CONTACT_PERSON_CONCAT_ID: String, COUNTRY_CODE: Option[String])

case class MatchResultAndContactPerson(
  matchResult: ContactPersonMatchingResult,
  contactPerson: ContactPersonRecord
) {
  val sourceId: String = matchResult.source_id
}

object ContactPersonMerging extends SparkJob {
  lazy private val sourcePreference = Source
    .fromInputStream(getClass.getResourceAsStream("/source_preference.tsv"))
    .getLines()
    .toSeq
    .filter(_.nonEmpty)
    .filterNot(_.equals("SOURCE\tPRIORITY"))
    .map(_.split("\t"))
    .map(lineParts => lineParts(0) -> lineParts(1).toInt)
    .toMap

  def pickGoldenRecordAndGroupId(contactPersons: Seq[ContactPersonRecord]): GoldenContactPersonRecord = {
    val refIds = contactPersons.map(_.CONTACT_PERSON_CONCAT_ID)
    val goldenRecord = contactPersons.reduce((o1, o2) => {
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
    GoldenContactPersonRecord(id, goldenRecord, refIds, goldenRecord.COUNTRY_CODE.get)
  }

  def transform(
    spark: SparkSession,
    contactPersons: Dataset[ContactPersonRecord],
    matches: Dataset[ContactPersonMatchingResult]
  ): Dataset[GoldenContactPersonRecord] = {
    import spark.implicits._

    val groupedContactPersons = matches
      .joinWith(
        contactPersons,
        matches("COUNTRY_CODE") === contactPersons("COUNTRY_CODE")
          and $"target_id" === $"CONTACT_PERSON_CONCAT_ID"
      )
      .map((MatchResultAndContactPerson.apply _).tupled)
      .groupByKey(_.sourceId)
      .agg(collect_list("contactPerson").alias("contactPersons").as[Seq[ContactPersonRecord]])
      .joinWith(contactPersons, $"value" === $"CONTACT_PERSON_CONCAT_ID")
      .map(x => x._2 +: x._1._2)

    val matchedIds = groupedContactPersons
      .flatMap(_.map(contactPerson => {
        ContactPersonIdAndCountry(contactPerson.CONTACT_PERSON_CONCAT_ID, contactPerson.COUNTRY_CODE)
      }))
      .distinct()

    val singletonContactPersons = contactPersons
      .join(matchedIds, Seq("CONTACT_PERSON_CONCAT_ID"), "leftanti")
      .as[ContactPersonRecord]
      .map(Seq(_))

    groupedContactPersons.union(singletonContactPersons).map(pickGoldenRecordAndGroupId)
  }

  override val neededFilePaths: Array[String] = Array(
    "MATCHING_INPUT_FILE",
    "CONTACT_PERSON_INPUT_FILE",
    "OUTPUT_FILE"
  )

  override def run(spark: SparkSession, filePaths: Product, storage: Storage): Unit = {
    import spark.implicits._

    val (matchingInputFile: String, contactPersonInputFile: String, outputFile: String) = filePaths

    val contactPersons = storage
      .readFromParquet[ContactPersonRecord](contactPersonInputFile)

    val matches = storage
      .readFromParquet[ContactPersonMatchingResult](matchingInputFile)

    val transformed = transform(spark, contactPersons, matches).repartition(60)

    storage
      .writeToParquet(transformed, outputFile, partitionBy = "COUNTRY_CODE")
  }
}
