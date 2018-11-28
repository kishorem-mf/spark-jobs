package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.{ DefaultConfig, SparkJobWithDefaultDbConfig }
import com.unilever.ohub.spark.domain.entity.ContactPerson
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.DomainDataProvider
import org.apache.spark.sql.{ Dataset, SparkSession }
import org.apache.spark.sql.functions._

object ContactPersonUpdateGoldenRecord extends SparkJobWithDefaultDbConfig with GoldenRecordPicking[ContactPerson] {

  def markGoldenRecord(sourcePreference: Map[String, Int])(contactPersons: Seq[ContactPerson]): Seq[ContactPerson] = {
    val goldenRecord = pickGoldenRecord(sourcePreference, contactPersons)
    contactPersons.map(o ⇒ o.copy(isGoldenRecord = o == goldenRecord))
  }

  def transform(
    spark: SparkSession,
    contactPersons: Dataset[ContactPerson],
    sourcePreference: Map[String, Int]
  ): Dataset[ContactPerson] = {
    import spark.implicits._

    contactPersons
      .map(x ⇒ x.ohubId.get -> x)
      .toDF("ohubId", "contactPerson")
      .groupBy("ohubId")
      .agg(collect_list($"contactPerson").as("contactPersons"))
      .as[(String, Seq[ContactPerson])]
      .map(_._2)
      .flatMap(markGoldenRecord(sourcePreference))
  }

  override def run(spark: SparkSession, config: DefaultConfig, storage: Storage): Unit = {
    run(spark, config, storage, DomainDataProvider(spark))
  }

  protected[merging] def run(spark: SparkSession, config: DefaultConfig, storage: Storage, dataProvider: DomainDataProvider): Unit = {
    val contactPersons = storage.readFromParquet[ContactPerson](config.inputFile)
    val transformed = transform(spark, contactPersons, dataProvider.sourcePreferences)

    storage.writeToParquet(transformed, config.outputFile)
  }
}
