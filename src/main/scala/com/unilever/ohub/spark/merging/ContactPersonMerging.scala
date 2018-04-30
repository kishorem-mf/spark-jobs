package com.unilever.ohub.spark.merging

import java.util.UUID

import com.unilever.ohub.spark.{ DefaultConfig, SparkJobWithDefaultConfig }
import com.unilever.ohub.spark.domain.entity.ContactPerson
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.tsv2parquet.DomainDataProvider
import org.apache.spark.sql.{ Dataset, SparkSession }

object ContactPersonMerging extends SparkJobWithDefaultConfig with GoldenRecordPicking[ContactPerson] {

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
      // TODO what if both are undefined, then we loose contact persons here (what about adding a constraint in the domain)? this legacy code...
      // we shouldn't loose data here...check whether the contact persons is the full list of contact persons (including ones without email address)
      .filter(cpn ⇒ cpn.emailAddress.isDefined || cpn.mobileNumber.isDefined)
      .groupByKey(cpn ⇒ cpn.emailAddress.getOrElse("") + cpn.mobileNumber.getOrElse(""))
      .flatMapGroups {
        case (_, contactPersonsIt) ⇒ markGoldenRecordAndGroupId(sourcePreference)(contactPersonsIt.toSeq)
      }
      .repartition(60)
  }

  override def run(spark: SparkSession, config: DefaultConfig, storage: Storage): Unit = {
    run(spark, config, storage, DomainDataProvider(spark))
  }

  protected[merging] def run(spark: SparkSession, config: DefaultConfig, storage: Storage, dataProvider: DomainDataProvider): Unit = {
    import spark.implicits._

    log.info(s"Merging contact persons from [${config.inputFile}] to [${config.outputFile}]")

    val contactPersons = storage.readFromParquet[ContactPerson](config.inputFile)
    val transformed = transform(spark, contactPersons, dataProvider.sourcePreferences)

    storage.writeToParquet(transformed, config.outputFile, partitionBy = Seq("countryCode"))
  }
}
