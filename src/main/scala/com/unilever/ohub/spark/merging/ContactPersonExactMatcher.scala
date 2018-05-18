package com.unilever.ohub.spark.merging

import java.util.UUID

import com.unilever.ohub.spark.{ DefaultWithDbConfig, SparkJobWithDefaultDbConfig }
import com.unilever.ohub.spark.domain.entity.ContactPerson
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.tsv2parquet.DomainDataProvider
import org.apache.spark.sql.{ Dataset, SparkSession }
import org.apache.spark.sql.functions._

object ContactPersonExactMatcher extends SparkJobWithDefaultDbConfig with GoldenRecordPicking[ContactPerson] {

  def markGoldenRecordAndGroupId(sourcePreference: Map[String, Int])(contactPersons: Seq[ContactPerson]): Seq[ContactPerson] = {
    val goldenRecord = pickGoldenRecord(sourcePreference, contactPersons)
    val groupId = UUID.randomUUID().toString
    contactPersons.map(o ⇒ o.copy(ohubId = Some(groupId), isGoldenRecord = o == goldenRecord))
  }

  def transform(
    spark: SparkSession,
    ingestedContactPersons: Dataset[ContactPerson],
    sourcePreference: Map[String, Int]
  ): Dataset[ContactPerson] = {
    import spark.implicits._

    ingestedContactPersons
      .filter(cpn ⇒ cpn.emailAddress.isDefined || cpn.mobileNumber.isDefined)
      .map(cpn ⇒ (cpn.emailAddress.getOrElse("") + cpn.mobileNumber.getOrElse(""), cpn))
      .toDF("group", "contactPerson")
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

    log.info(s"Exact matching contact persons from [${config.inputFile}] to [${config.outputFile}]")

    val ingestedContactPersons = storage.readFromParquet[ContactPerson](config.inputFile)
    val transformed = transform(spark, ingestedContactPersons, dataProvider.sourcePreferences)

    storage.writeToParquet(transformed, config.outputFile)
  }
}
