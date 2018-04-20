package com.unilever.ohub.spark.merging

import java.util.UUID

import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.domain.entity.ContactPerson
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.tsv2parquet.DomainDataProvider
import org.apache.spark.sql.{ Dataset, SparkSession }

object ContactPersonMerging extends SparkJob with GoldenRecordPicking[ContactPerson] {

  def markGoldenRecordAndGroupId(sourcePreference: Map[String, Int])(contactPersons: Seq[ContactPerson]): Seq[ContactPerson] = {
    val goldenRecord = pickGoldenRecord(sourcePreference, contactPersons)
    val groupId = UUID.randomUUID().toString
    contactPersons.map(o ⇒ o.copy(ohubId = Some(groupId), isGoldenRecord = o == goldenRecord))
  }

  def transform(
    spark: SparkSession,
    contactPersons: Dataset[ContactPerson],
    sourcePreference: Map[String, Int]
  ): Dataset[ContactPerson] = {
    import spark.implicits._

    contactPersons
      // TODO what if both are undefined, then we loose contact persons here (what about adding a constraint in the domain)?
      .filter(cpn ⇒ cpn.emailAddress.isDefined || cpn.mobileNumber.isDefined)
      .groupByKey(cpn ⇒ cpn.emailAddress.getOrElse("") + cpn.mobileNumber.getOrElse(""))
      .flatMapGroups {
        case (_, contactPersonsIt) ⇒ markGoldenRecordAndGroupId(sourcePreference)(contactPersonsIt.toSeq)
      }
      .repartition(60)
  }

  override val neededFilePaths: Array[String] = Array("INPUT_FILE", "OUTPUT_FILE")

  override def run(spark: SparkSession, filePaths: Product, storage: Storage): Unit = {
    run(spark, filePaths, storage, DomainDataProvider(spark))
  }

  protected[merging] def run(spark: SparkSession, filePaths: Product, storage: Storage, dataProvider: DomainDataProvider): Unit = {
    import spark.implicits._

    val (inputFile: String, outputFile: String) = filePaths
    log.info(s"Merging contact persons from [$inputFile] to [$outputFile]")

    val contactPersons = storage.readFromParquet[ContactPerson](inputFile)
    val transformed = transform(spark, contactPersons, dataProvider.sourcePreferences)

    storage.writeToParquet(transformed, outputFile, partitionBy = Seq("countryCode"))
  }
}
