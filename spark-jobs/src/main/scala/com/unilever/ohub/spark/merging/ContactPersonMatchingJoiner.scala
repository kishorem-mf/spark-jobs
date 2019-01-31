package com.unilever.ohub.spark.merging

import java.sql.Timestamp

import com.unilever.ohub.spark.DomainDataProvider
import com.unilever.ohub.spark.domain.entity.ContactPerson
import org.apache.spark.sql.{ Dataset, SparkSession }

object ContactPersonMatchingJoiner extends BaseMatchingJoiner[ContactPerson] {

  // TODO resolve code duplication...but for now...first make it work (again), then make it better
  override def transform(
    spark: SparkSession,
    entities: Dataset[ContactPerson],
    matches: Dataset[MatchingResult],
    dataProvider: DomainDataProvider
  ): Dataset[ContactPerson] = {
    import spark.implicits._

    val matchedOperators: Dataset[Seq[ContactPerson]] = groupMatchedEntities(spark, entities, matches)
    val unmatchedOperators: Dataset[Seq[ContactPerson]] = findUnmatchedEntities(spark, entities, matchedOperators)
    val markGoldenRecordAndGroupIdFn: Seq[ContactPerson] ⇒ Seq[ContactPerson] = markGoldenRecordAndGroupId(dataProvider.sourcePreferences)

    matchedOperators
      .union(unmatchedOperators)
      .flatMap(markGoldenRecordAndGroupIdFn)
  }

  // When it is decided to select golden record based on source instead of newest, remove
  // this override def pickGoldenRecord(...
  /**
   * Get the newest contactPerson(based on dateUpdated, dateCreated and ohubUpdated) to mark as golden record.
   * @param sourcePreference -- not used
   * @param entities
   * @return
   */
  override def pickGoldenRecord(sourcePreference: Map[String, Int], entities: Seq[ContactPerson]): ContactPerson = {
    implicit def ordered: Ordering[Timestamp] = new Ordering[Timestamp] {
      def compare(x: Timestamp, y: Timestamp): Int = x compareTo y
    }
    entities.sortBy(cp ⇒ (cp.dateUpdated, cp.dateCreated, cp.ohubUpdated)).reverse.head
  }

  override private[merging] def markGoldenAndGroup(entity: ContactPerson, isGoldenRecord: Boolean, groupId: String): ContactPerson = {
    entity.copy(ohubId = Some(groupId), isGoldenRecord = isGoldenRecord)
  }
}
