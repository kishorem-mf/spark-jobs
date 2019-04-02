package com.unilever.ohub.spark.merging

import java.sql.Timestamp

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

  // When it is decided to select golden record based on source instead of newest, remove
  // this override def pickGoldenRecord(...
  /**
   * Get the newest contactPerson(based on dateUpdated, dateCreated, ohubUpdated and isGoldenRecord) to mark as golden record.
   *
   * @param sourcePreference -- not used
   * @param entities
   * @return
   */
  override def pickGoldenRecord(sourcePreference: Map[String, Int], entities: Seq[ContactPerson]): ContactPerson = {

    implicit def ordered: Ordering[Timestamp] = new Ordering[Timestamp] {
      def compare(x: Timestamp, y: Timestamp): Int = x compareTo y
    }

    val newest = entities.sortBy(cp ⇒ (cp.dateUpdated, cp.dateCreated, cp.ohubUpdated)).reverse.head

    val newestCPs = entities.filter((c) ⇒
      c.dateUpdated == newest.dateUpdated &&
        c.dateCreated == newest.dateCreated &&
        c.ohubUpdated == newest.ohubUpdated
    )

    // If there is 1 or more golden records with the newest dates that is golden, pick one of those
    val newestGolden = newestCPs.filter(_.isGoldenRecord)
    if (newestGolden.size > 0) newestGolden(0)
    else newest
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
