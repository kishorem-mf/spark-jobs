package com.unilever.ohub.spark.merging

import java.util.UUID

import com.unilever.ohub.spark.{ SparkJob, SparkJobConfig }
import com.unilever.ohub.spark.domain.entity.ContactPerson
import com.unilever.ohub.spark.sql.JoinType
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.DomainDataProvider
import org.apache.spark.sql.{ Dataset, SparkSession }
import org.apache.spark.sql.functions._
import scopt.OptionParser

case class ExactMatchWithDbConfig(
    inputFile: String = "path-to-input-file",
    exactMatchOutputFile: String = "path-to-exact-match-output-file",
    leftOversOutputFile: String = "path-to-left-overs-output-file"
) extends SparkJobConfig

object ContactPersonExactMatcher extends SparkJob[ExactMatchWithDbConfig] with GoldenRecordPicking[ContactPerson] {

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

  def leftOversForFuzzyMatching(
    spark: SparkSession,
    ingestedContactPersons: Dataset[ContactPerson],
    exactMatchedContactPersons: Dataset[ContactPerson]
  ): Dataset[ContactPerson] = {
    import spark.implicits._

    ingestedContactPersons
      .join(exactMatchedContactPersons, Seq("concatId"), JoinType.LeftAnti)
      .as[ContactPerson]
  }

  override private[spark] def defaultConfig = ExactMatchWithDbConfig()

  override private[spark] def configParser(): OptionParser[ExactMatchWithDbConfig] =
    new scopt.OptionParser[ExactMatchWithDbConfig]("Spark job default") {
      head("run a spark job with default config.", "1.0")
      opt[String]("inputFile") required () action { (x, c) ⇒
        c.copy(inputFile = x)
      } text "inputFile is a string property"
      opt[String]("exactMatchOutputFile") required () action { (x, c) ⇒
        c.copy(exactMatchOutputFile = x)
      } text "exactMatchOutputFile is a string property"
      opt[String]("leftOversOutputFile") required () action { (x, c) ⇒
        c.copy(leftOversOutputFile = x)
      } text "leftOversOutputFile is a string property"

      version("1.0")
      help("help") text "help text"
    }

  override def run(spark: SparkSession, config: ExactMatchWithDbConfig, storage: Storage): Unit = {
    run(spark, config, storage, DomainDataProvider(spark))
  }

  protected[merging] def run(spark: SparkSession, config: ExactMatchWithDbConfig, storage: Storage, dataProvider: DomainDataProvider): Unit = {
    import spark.implicits._

    log.info(s"Exact matching contact persons from [${config.inputFile}] to [${config.exactMatchOutputFile}] and left-overs to [${config.leftOversOutputFile}]")

    val ingestedContactPersons = storage.readFromParquet[ContactPerson](config.inputFile)
    val exactMatches = transform(spark, ingestedContactPersons, dataProvider.sourcePreferences)
    val fuzzyMatchContactPersons = leftOversForFuzzyMatching(spark, ingestedContactPersons, exactMatches)

    storage.writeToParquet(exactMatches, config.exactMatchOutputFile)
    storage.writeToParquet(fuzzyMatchContactPersons, config.leftOversOutputFile)
  }
}
