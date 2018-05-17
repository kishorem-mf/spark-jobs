package com.unilever.ohub.spark.merging

import java.util.UUID

import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.domain.entity.Operator
import com.unilever.ohub.spark.sql.JoinType
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.tsv2parquet.DomainDataProvider
import org.apache.spark.sql.{ Dataset, SparkSession }
import org.apache.spark.sql.functions._
import scopt.OptionParser

// TODO resolve code duplication...but for now...first make it work (again), then make it better

object OperatorMatchingJoiner extends SparkJob[DomainEntityJoinConfig] with GoldenRecordPicking[Operator] {

  private case class ConcatId(concatId: String)

  private case class MatchingResultAndOperator(
      matchingResult: MatchingResult,
      operator: Operator
  ) {
    val sourceId: String = matchingResult.sourceId
  }

  def transform(
    spark: SparkSession,
    operators: Dataset[Operator],
    matches: Dataset[MatchingResult],
    markGoldenRecordsFunction: Seq[Operator] ⇒ Seq[Operator]
  ): Dataset[Operator] = {
    import spark.implicits._

    val matchedOperators: Dataset[Seq[Operator]] = groupMatchedEntities(spark, operators, matches)
    val unmatchedOperators: Dataset[Seq[Operator]] = findUnmatchedEntities(spark, operators, matchedOperators)

    matchedOperators
      .union(unmatchedOperators)
      .flatMap(markGoldenRecordsFunction)
  }

  private[merging] def groupMatchedEntities(
    spark: SparkSession,
    allOperators: Dataset[Operator],
    matches: Dataset[MatchingResult]): Dataset[Seq[Operator]] = {
    import spark.implicits._

    matches
      .joinWith(allOperators, matches("targetId") === allOperators("concatId"), JoinType.Inner)
      .toDF("matchingResult", "entity")
      .groupBy($"matchingResult.sourceId")
      .agg(collect_list("entity").as("entities"))
      .as[(String, Seq[Operator])]
      .joinWith(allOperators, $"sourceId" === $"concatId", JoinType.Inner)
      .map { case ((_, entities), entity) ⇒ entity +: entities }
  }

  private[merging] def findUnmatchedEntities(
    spark: SparkSession,
    allOperators: Dataset[Operator],
    matched: Dataset[Seq[Operator]]) = {

    import spark.implicits._

    val matchedIds = matched
      .flatMap(_.map(c ⇒ c.concatId))
      .toDF("concatId")
      .distinct

    allOperators
      .join(matchedIds, Seq("concatId"), JoinType.LeftAnti)
      .as[Operator]
      .map(Seq(_))
  }

  private[merging] def markGoldenRecordAndGroupId(sourcePreference: Map[String, Int])(operators: Seq[Operator]): Seq[Operator] = {
    val goldenRecord = pickGoldenRecord(sourcePreference, operators)
    val groupId = UUID.randomUUID().toString
    operators.map(o ⇒ o.copy(ohubId = Some(groupId), isGoldenRecord = o == goldenRecord))
  }

  override private[spark] def defaultConfig = DomainEntityJoinConfig()

  override private[spark] def configParser(): OptionParser[DomainEntityJoinConfig] =
    new scopt.OptionParser[DomainEntityJoinConfig]("Domain entity joining") {
      head("joining entities into an intermediate output file", "1.0")
      opt[String]("matchingInputFile") required () action { (x, c) ⇒
        c.copy(matchingInputFile = x)
      } text "matchingInputFile is a string property"
      opt[String]("entityInputFile") required () action { (x, c) ⇒
        c.copy(entityInputFile = x)
      } text "entityInputFile is a string property"
      opt[String]("outputFile") required () action { (x, c) ⇒
        c.copy(outputFile = x)
      } text "outputFile is a string property"
      opt[String]("postgressUrl") required () action { (x, c) ⇒
        c.copy(postgressUrl = x)
      } text "postgressUrl is a string property"
      opt[String]("postgressUsername") required () action { (x, c) ⇒
        c.copy(postgressUsername = x)
      } text "postgressUsername is a string property"
      opt[String]("postgressPassword") required () action { (x, c) ⇒
        c.copy(postgressPassword = x)
      } text "postgressPassword is a string property"
      opt[String]("postgressDB") required () action { (x, c) ⇒
        c.copy(postgressDB = x)
      } text "postgressDB is a string property"

      version("1.0")
      help("help") text "help text"
    }

  override def run(spark: SparkSession, config: DomainEntityJoinConfig, storage: Storage): Unit = {
    run(spark, config, storage, DomainDataProvider(spark, config.postgressUrl, config.postgressDB, config.postgressUsername, config.postgressPassword))
  }

  protected[merging] def run(spark: SparkSession, config: DomainEntityJoinConfig, storage: Storage, dataProvider: DomainDataProvider): Unit = {
    import spark.implicits._

    log.info(s"Merging operators from [${config.matchingInputFile}] and [${config.entityInputFile}] to [${config.outputFile}]")

    val operators = storage.readFromParquet[Operator](config.entityInputFile)

    val matches = storage
      .readFromParquet[MatchingResult](
        config.matchingInputFile,
        selectColumns = Seq(
          $"sourceId",
          $"targetId",
          $"countryCode"
        )
      )

    val transformed = transform(spark, operators, matches, markGoldenRecordAndGroupId(dataProvider.sourcePreferences))

    storage.writeToParquet(transformed, config.outputFile)
  }
}
