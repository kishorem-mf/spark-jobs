package com.unilever.ohub.spark.merging

import java.util.UUID

import com.unilever.ohub.spark.{ SparkJob, SparkJobConfig }
import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.sql.JoinType
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.DomainDataProvider
import org.apache.spark.sql.{ Dataset, SparkSession }
import org.apache.spark.sql.functions._
import scopt.OptionParser

import scala.reflect.runtime.universe._

case class MatchingResult(sourceId: String, targetId: String)

case class DomainEntityJoinConfig(
    matchingInputFile: String = "matching-input-file",
    entityInputFile: String = "entity-input-file",
    outputFile: String = "path-to-output-file",
    postgressUrl: String = "postgress-url",
    postgressUsername: String = "postgress-username",
    postgressPassword: String = "postgress-password",
    postgressDB: String = "postgress-db"
) extends SparkJobConfig

abstract class BaseMatchingJoiner[T <: DomainEntity: TypeTag] extends SparkJob[DomainEntityJoinConfig] with GoldenRecordPicking[T] {

  def transform(
    spark: SparkSession,
    entities: Dataset[T],
    matches: Dataset[MatchingResult],
    dataProvider: DomainDataProvider): Dataset[T]

  private[merging] def markGoldenAndGroup(entity: T, isGoldenRecord: Boolean, groupId: String): T

  private[merging] def markGoldenRecordAndGroupId(sourcePreference: Map[String, Int])(entities: Seq[T]): Seq[T] = {
    val goldenRecord = pickGoldenRecord(sourcePreference, entities)
    val groupId = UUID.randomUUID().toString
    entities.map(cp ⇒ markGoldenAndGroup(cp, cp == goldenRecord, groupId))
  }

  private[merging] def groupMatchedEntities(
    spark: SparkSession,
    allEntities: Dataset[T],
    matches: Dataset[MatchingResult]): Dataset[Seq[T]] = {
    import spark.implicits._

    matches
      .joinWith(allEntities, matches("targetId") === allEntities("concatId"), JoinType.Inner)
      .toDF("matchingResult", "entity")
      .groupBy($"matchingResult.sourceId")
      .agg(collect_list("entity").as("entities"))
      .as[(String, Seq[T])]
      .joinWith(allEntities, $"sourceId" === $"concatId", JoinType.Inner)
      .map { case ((_, entities), entity) ⇒ entity +: entities }
  }

  private[merging] def findUnmatchedEntities(
    spark: SparkSession,
    allEntities: Dataset[T],
    matched: Dataset[Seq[T]]): Dataset[Seq[T]] = {

    import spark.implicits._

    val matchedIds = matched
      .flatMap(_.map(c ⇒ c.concatId))
      .toDF("concatId")
      .distinct

    allEntities
      .join(matchedIds, Seq("concatId"), JoinType.LeftAnti)
      .as[T]
      .map(Seq(_))
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

    log.info(s"Merging domain entities from [${config.matchingInputFile}] and [${config.entityInputFile}] to [${config.outputFile}]")

    val entities = storage.readFromParquet[T](config.entityInputFile)

    val matches = storage
      .readFromParquet[MatchingResult](
        config.matchingInputFile,
        selectColumns = Seq(
          $"sourceId",
          $"targetId"
        )
      )

    val transformed = transform(spark, entities, matches, dataProvider)

    storage.writeToParquet(transformed, config.outputFile)
  }
}
