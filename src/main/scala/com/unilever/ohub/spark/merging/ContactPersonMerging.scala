package com.unilever.ohub.spark.merging

import java.sql.Timestamp
import java.util.UUID

import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.data.{ ContactPersonRecord, GoldenContactPersonRecord }
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{ Dataset, SparkSession }

object ContactPersonMerging extends SparkJob {
  def pickGoldenRecordAndGroupId(
    sourcePreference: Map[String, Int],
    contactPersons: Set[ContactPersonRecord]
  ): GoldenContactPersonRecord = {
    val refIds = contactPersons.map(_.contactPersonConcatId)
    val goldenRecord = contactPersons.reduce((o1, o2) => {
      val preference1 = sourcePreference.getOrElse(o1.source.getOrElse("UNKNOWN"), Int.MaxValue)
      val preference2 = sourcePreference.getOrElse(o2.source.getOrElse("UNKNOWN"), Int.MaxValue)
      if (preference1 < preference2) o1
      else if (preference1 > preference2) o2
      else { // same source preference
        val created1 = o1.dateCreated.getOrElse(new Timestamp(System.currentTimeMillis))
        val created2 = o1.dateCreated.getOrElse(new Timestamp(System.currentTimeMillis))
        if (created1.before(created2)) o1 else o2
      }
    })
    val id = UUID.randomUUID().toString
    GoldenContactPersonRecord(id, goldenRecord, refIds.toSeq, goldenRecord.countryCode)
  }

  def transform(
    spark: SparkSession,
    contactPersons: Dataset[ContactPersonRecord],
    sourcePreference: Map[String, Int]
  ): Dataset[GoldenContactPersonRecord] = {
    import spark.implicits._

    contactPersons
      .filter(cpn => cpn.emailAddress.nonEmpty || cpn.mobilePhoneNumber.nonEmpty)
      .groupByKey(cpn => cpn.emailAddress.getOrElse("") + cpn.mobilePhoneNumber.getOrElse(""))
      .mapGroups {
        case (_, contactPersonsIt) => pickGoldenRecordAndGroupId(sourcePreference, contactPersonsIt.toSet)
      }
      .repartition(60)
  }

  override val neededFilePaths: Array[String] = Array("INPUT_FILE", "OUTPUT_FILE")

  override def run(spark: SparkSession, filePaths: Product, storage: Storage): Unit = {
    import spark.implicits._

    val (inputFile: String, outputFile: String) = filePaths

    log.info(s"Merging contact persons from [$inputFile] to [$outputFile]")

    val contactPersons = storage
      .readFromParquet[ContactPersonRecord](inputFile)

    val transformed = transform(spark, contactPersons, storage.sourcePreference)

    storage
      .writeToParquet(transformed, outputFile, partitionBy = "countryCode")
  }
}
