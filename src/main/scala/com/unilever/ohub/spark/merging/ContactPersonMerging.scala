package com.unilever.ohub.spark.merging

import java.util.UUID

import com.unilever.ohub.spark.{ DefaultWithDbConfig, SparkJobWithDefaultDbConfig }
import com.unilever.ohub.spark.domain.entity.ContactPerson
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.tsv2parquet.DomainDataProvider
import org.apache.spark.sql.{ Dataset, SparkSession }
import org.apache.spark.sql.functions._

case class GroupObject(group: String, contactPerson: ContactPerson)

object ContactPersonMerging extends SparkJobWithDefaultDbConfig with GoldenRecordPicking[ContactPerson] {

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
      .filter(cpn ⇒ cpn.emailAddress.isDefined || cpn.mobileNumber.isDefined)
      .map(cpn ⇒ GroupObject(cpn.emailAddress.getOrElse("") + cpn.mobileNumber.getOrElse(""), cpn))
      .groupBy($"group")
      .agg(collect_list("contactPerson").as("contactPersons"))
      .as[(String, Seq[ContactPerson])]
      .flatMap {
        case (_, contactPersonList) ⇒ markGoldenRecordAndGroupId(sourcePreference)(contactPersonList)
      }
  }

  override def run(spark: SparkSession, config: DefaultWithDbConfig, storage: Storage): Unit = {
    run(spark, config, storage, DomainDataProvider(spark, config.postgressUrl, config.postgressDB, config.postgressUsername, config.postgressPassword))
  }

  protected[merging] def run(spark: SparkSession, config: DefaultWithDbConfig, storage: Storage, dataProvider: DomainDataProvider): Unit = {
    import spark.implicits._

    log.info(s"Merging contact persons from [${config.inputFile}] to [${config.outputFile}]")

    val contactPersons = storage.readFromParquet[ContactPerson](config.inputFile)
    val transformed = transform(spark, contactPersons, dataProvider.sourcePreferences)

    storage.writeToParquet(transformed, config.outputFile)
  }
}
