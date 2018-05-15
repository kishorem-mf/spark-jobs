package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.{SparkJob, SparkJobConfig}
import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.sql.JoinType
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

case class MatchingResult(sourceId: String, targetId: String, countryCode: String)

abstract class BaseMatchingJoiner[T <: DomainEntity, C: SparkJobConfig] extends SparkJob[C] with GoldenRecordPicking[T] {

  private case class MatchingResultAndDomainEntity(
                                                    matchingResult: MatchingResult,
                                                    entity: T
                                                  ) {
    val sourceId: String = matchingResult.sourceId
  }

  private case class ConcatId(concatId: String)

  def transform(
                 spark: SparkSession,
                 entities: Dataset[T],
                 matches: Dataset[MatchingResult],
                 markGoldenRecordsFunction: Seq[T] => Seq[T]):
  Dataset[T] = {
    import spark.implicits._

    val matchedEntities: Dataset[Seq[T]] = groupMatchedEntities(spark, entities, matches)
    val unmatchedEntities: Dataset[Seq[T]] = findUnmatchedEntities(spark, entities, matchedEntities)

    matchedEntities
      .union(unmatchedEntities)
      .flatMap(markGoldenRecordsFunction)
  }

  private[merging] def groupMatchedEntities(

                                             spark: SparkSession,
                                             allEntities: Dataset[T],
                                             matches: Dataset[MatchingResult]): Dataset[Seq[T]] = {
    import spark.implicits._

    matches
      .joinWith(allEntities, matches("targetId") === allEntities("concatId"), JoinType.Inner)
      .map((MatchingResultAndDomainEntity.apply _).tupled)
      .groupBy($"matchingResult.sourceId")
      .agg(collect_list("entity").as("entities"))
      .as[(String, Seq[T])]
      .joinWith(allEntities, $"sourceId" === $"concatId", JoinType.Inner)
      .map { case ((_, entities), entity) ⇒ entity +: entities }
  }

  private[merging] def findUnmatchedEntities(
                                              spark: SparkSession,
                                              allEntities: Dataset[T],
                                              matched: Dataset[Seq[T]]) = {

    import spark.implicits._

    val matchedIds = matched
      .flatMap(_.map(c ⇒ ConcatId(c.concatId)))
      .as[ConcatId]
      .distinct

    allEntities
      .join(matchedIds, Seq("concatId"), JoinType.LeftAnti)
      .as[T]
      .map(Seq(_))
  }
}
