package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.domain.entity.ContactPerson
import com.unilever.ohub.spark.DomainDataProvider
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

  override private[merging] def markGoldenAndGroup(entity: ContactPerson, isGoldenRecord: Boolean, groupId: String): ContactPerson = {
    entity.copy(ohubId = Some(groupId), isGoldenRecord = isGoldenRecord)
  }
}
