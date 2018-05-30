package com.unilever.ohub.spark.merging

import java.util.UUID

import com.unilever.ohub.spark.domain.entity.ContactPerson
import com.unilever.ohub.spark.sql.JoinType
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.{ SparkJob, SparkJobConfig }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ Dataset, SparkSession }
import scopt.OptionParser

case class ExactMatchIngestedWithDbConfig(
    integratedInputFile: String = "path-to-integrated-input-file",
    deltaInputFile: String = "path-to-delta-input-file",
    matchedExact: String = "path-to-matched-exact",
    unmatchedIntegrated: String = "path-to-unmatched-integrated",
    unmatchedDelta: String = "path-to-unmatched-delta",
    postgressUrl: String = "postgress-url",
    postgressUsername: String = "postgress-username",
    postgressPassword: String = "postgress-password",
    postgressDB: String = "postgress-db"
) extends SparkJobConfig

object ContactPersonIntegratedExactMatch extends SparkJob[ExactMatchIngestedWithDbConfig] {

  override private[spark] def defaultConfig = ExactMatchIngestedWithDbConfig()

  override private[spark] def configParser(): OptionParser[ExactMatchIngestedWithDbConfig] =
    new scopt.OptionParser[ExactMatchIngestedWithDbConfig]("Spark job default") {
      head("run a spark job with default config.", "1.0")
      opt[String]("integratedInputFile") required () action { (x, c) ⇒
        c.copy(integratedInputFile = x)
      } text "integratedInputFile is a string property"
      opt[String]("deltaInputFile") required () action { (x, c) ⇒
        c.copy(deltaInputFile = x)
      } text "deltaInputFile is a string property"
      opt[String]("matchedExact") required () action { (x, c) ⇒
        c.copy(matchedExact = x)
      } text "matchedExact is a string property"
      opt[String]("unmatchedIntegrated") required () action { (x, c) ⇒
        c.copy(unmatchedIntegrated = x)
      } text "unmatchedIntegrated is a string property"
      opt[String]("unmatchedDelta") required () action { (x, c) ⇒
        c.copy(unmatchedDelta = x)
      } text "unmatchedDelta is a string property"
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

  def transform(spark: SparkSession, integratedContactPersons: Dataset[ContactPerson], dailyDeltaContactPersons: Dataset[ContactPerson]): (Dataset[ContactPerson], Dataset[ContactPerson], Dataset[ContactPerson]) = {
    import spark.implicits._

    val matchedExact: Dataset[ContactPerson] = determineExactMatches(spark, integratedContactPersons, dailyDeltaContactPersons)

    val unmatchedIntegrated = integratedContactPersons
      .join(matchedExact, Seq("concatId"), JoinType.LeftAnti)
      .as[ContactPerson]

    val unmatchedDelta = dailyDeltaContactPersons
      .join(matchedExact, Seq("concatId"), JoinType.LeftAnti)
      .as[ContactPerson]

    (matchedExact, unmatchedIntegrated, unmatchedDelta)
  }

  private def determineExactMatches(spark: SparkSession, integratedContactPersons: Dataset[ContactPerson], dailyDeltaContactPersons: Dataset[ContactPerson]): Dataset[ContactPerson] = {
    import spark.implicits._

    lazy val integratedWithExact = integratedContactPersons
      .filter('emailAddress.isNotNull || 'mobileNumber.isNotNull)
      .map(cpn ⇒ (cpn.emailAddress.getOrElse("") + cpn.mobileNumber.getOrElse(""), cpn))
      .toDF("group", "contactPerson")
      .withColumn("inDelta", lit(false))

    lazy val newWithExact =
      dailyDeltaContactPersons
        .filter('emailAddress.isNotNull || 'mobileNumber.isNotNull)
        .map(cpn ⇒ (cpn.emailAddress.getOrElse("") + cpn.mobileNumber.getOrElse(""), cpn))
        .toDF("group", "contactPerson")
        .withColumn("inDelta", lit(true))

    integratedWithExact
      .union(newWithExact)
      .groupBy($"group")
      .agg(collect_list(struct($"contactPerson", $"inDelta")).as("contactPersons"))
      .as[(String, Seq[(ContactPerson, Boolean)])]
      .flatMap { // first set the proper ohubId
        case (_, contactPersonList) ⇒
          val ohubId: String = contactPersonList.find {
            case (contactPerson, _) ⇒ contactPerson.ohubId.isDefined
          }.flatMap {
            case (contactPerson, _) ⇒ contactPerson.ohubId
          }.getOrElse(UUID.randomUUID().toString)

          contactPersonList.map {
            case (contactPerson, inDelta) ⇒ (contactPerson.copy(ohubId = Some(ohubId)), inDelta)
          }
      }
      .groupByKey { // TODO resolve this
        case (contactPerson, _) ⇒ contactPerson.concatId
      }
      .reduceGroups((left, right) ⇒
        if (left._2) left else right
      )
      .map {
        case (_, (contactPerson, _)) ⇒ contactPerson
      }
  }

  override def run(spark: SparkSession, config: ExactMatchIngestedWithDbConfig, storage: Storage): Unit = {
    import spark.implicits._

    log.info(s"Integrated vs ingested exact matching contact persons from [${config.integratedInputFile}] and " +
      s"[${config.deltaInputFile}] to matched exact output [${config.matchedExact}], unmatched integrated output to [${config.unmatchedIntegrated}] and" +
      s"unmatched delta output [${config.unmatchedDelta}]")

    val integratedContactPersons = storage.readFromParquet[ContactPerson](config.integratedInputFile)
    val dailyDeltaContactPersons = storage.readFromParquet[ContactPerson](config.deltaInputFile)

    val (matchedExact, unmatchedIntegrated, unmatchedDelta) = transform(spark, integratedContactPersons, dailyDeltaContactPersons)

    storage.writeToParquet(matchedExact, config.matchedExact)
    storage.writeToParquet(unmatchedIntegrated, config.unmatchedIntegrated)
    storage.writeToParquet(unmatchedDelta, config.unmatchedDelta)
  }
}
