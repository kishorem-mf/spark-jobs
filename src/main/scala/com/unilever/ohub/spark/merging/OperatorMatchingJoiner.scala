package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.domain.entity.Operator
import com.unilever.ohub.spark.DomainDataProvider
import org.apache.spark.sql.{ Dataset, SparkSession }

object OperatorMatchingJoiner extends BaseMatchingJoiner[Operator] {

  // TODO resolve code duplication...but for now...first make it work (again), then make it better
  override def transform(
    spark: SparkSession,
    entities: Dataset[Operator],
    matches: Dataset[MatchingResult],
    dataProvider: DomainDataProvider
  ): Dataset[Operator] = {
    import spark.implicits._

    val matchedOperators: Dataset[Seq[Operator]] = groupMatchedEntities(spark, entities, matches)
    val unmatchedOperators: Dataset[Seq[Operator]] = findUnmatchedEntities(spark, entities, matchedOperators)
    val markGoldenRecordAndGroupIdFn: Seq[Operator] ⇒ Seq[Operator] = markGoldenRecordAndGroupId(dataProvider.sourcePreferences)

    matchedOperators
      .union(unmatchedOperators)
      .flatMap(markGoldenRecordAndGroupIdFn)
  }

  override private[merging] def markGoldenAndGroup(entity: Operator, isGoldenRecord: Boolean, groupId: String): Operator = {
    entity.copy(ohubId = Some(groupId), isGoldenRecord = isGoldenRecord)
  }
}
