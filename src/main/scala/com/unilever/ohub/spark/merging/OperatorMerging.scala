package com.unilever.ohub.spark.merging

import java.util.UUID

import com.unilever.ohub.spark.{ SparkJob, SparkJobConfig }
import com.unilever.ohub.spark.domain.entity.Operator
import com.unilever.ohub.spark.sql.JoinType
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.tsv2parquet.DomainDataProvider
import org.apache.spark.sql.{ Dataset, SparkSession }
import org.apache.spark.sql.functions.collect_list
import scopt.OptionParser

case class OperatorMergingConfig(
    matchingInputFile: String = "matching-input-file",
    operatorInputFile: String = "operator-input-file",
    outputFile: String = "path-to-output-file",
    postgressUrl: String = "postgress-url",
    postgressUsername: String = "postgress-username",
    postgressPassword: String = "postgress-password",
    postgressDB: String = "postgress-db"
) extends SparkJobConfig

case class MatchingResult(sourceId: String, targetId: String, countryCode: String)

object OperatorMerging extends SparkJob[OperatorMergingConfig] with GoldenRecordPicking[Operator] {

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
    sourcePreference: Map[String, Int]
  ): Dataset[Operator] = {
    import spark.implicits._

    val matchedOperators: Dataset[Seq[Operator]] = groupMatchedOperators(spark, operators, matches)
    val unmatchedOperators: Dataset[Seq[Operator]] = findUnmatchedOperators(spark, operators, matchedOperators)

    matchedOperators
      .union(unmatchedOperators)
      .flatMap(markGoldenRecordAndGroupId(sourcePreference))
  }

  private[merging] def groupMatchedOperators(
    spark: SparkSession,
    allOperators: Dataset[Operator],
    matches: Dataset[MatchingResult]): Dataset[Seq[Operator]] = {
    import spark.implicits._
    matches
      .joinWith(allOperators, matches("targetId") === allOperators("concatId"), JoinType.Inner)
      .map((MatchingResultAndOperator.apply _).tupled)
      .groupByKey(_.sourceId)
      .agg(collect_list("operator").alias("operators").as[Seq[Operator]])
      .joinWith(allOperators, $"value" === $"concatId", JoinType.Inner)
      .map { case ((_, operators), operator) ⇒ operator +: operators }
  }

  private[merging] def findUnmatchedOperators(
    spark: SparkSession,
    allOperators: Dataset[Operator],
    matched: Dataset[Seq[Operator]]) = {

    import spark.implicits._

    val matchedIds = matched
      .flatMap(_.map(c ⇒ ConcatId(c.concatId)))
      .as[ConcatId]
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

  override private[spark] def defaultConfig = OperatorMergingConfig()

  override private[spark] def configParser(): OptionParser[OperatorMergingConfig] =
    new scopt.OptionParser[OperatorMergingConfig]("Operator merging") {
      head("merges operators into an integrated operator output file", "1.0")
      opt[String]("matchingInputFile") required () action { (x, c) ⇒
        c.copy(matchingInputFile = x)
      } text "matchingInputFile is a string property"
      opt[String]("operatorInputFile") required () action { (x, c) ⇒
        c.copy(operatorInputFile = x)
      } text "operatorInputFile is a string property"
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

  override def run(spark: SparkSession, config: OperatorMergingConfig, storage: Storage): Unit = {
    run(spark, config, storage, DomainDataProvider(spark, config.postgressUrl, config.postgressDB, config.postgressUsername, config.postgressPassword))
  }

  protected[merging] def run(spark: SparkSession, config: OperatorMergingConfig, storage: Storage, dataProvider: DomainDataProvider): Unit = {
    import spark.implicits._

    log.info(s"Merging operators from [${config.matchingInputFile}] and [${config.operatorInputFile}] to [${config.outputFile}]")

    val operators = storage.readFromParquet[Operator](config.operatorInputFile)

    val matches = storage
      .readFromParquet[MatchingResult](
        config.matchingInputFile,
        selectColumns = Seq(
          $"sourceId",
          $"targetId",
          $"countryCode"
        )
      )

    val transformed = transform(spark, operators, matches, dataProvider.sourcePreferences)

    storage.writeToParquet(transformed, config.outputFile)
  }
}
