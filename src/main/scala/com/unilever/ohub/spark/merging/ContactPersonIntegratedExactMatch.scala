package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.domain.entity.ContactPerson
import com.unilever.ohub.spark.sql.JoinType
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.{ SparkJob, SparkJobConfig }
import org.apache.spark.sql.{ Dataset, SparkSession }
import scopt.OptionParser

case class ExactMatchIngestedWithDbConfig(
    integratedInputFile: String = "path-to-integrated-input-file",
    deltaInputFile: String = "path-to-delta-input-file",
    exactMatchOutputFile: String = "path-to-exact-match-output-file",
    unmatchedDeltaOutputFile: String = "path-to-unmatched-delta-output-file",
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
      opt[String]("exactMatchOutputFile") required () action { (x, c) ⇒
        c.copy(exactMatchOutputFile = x)
      } text "exactMatchOutputFile is a string property"
      opt[String]("unmatchedDeltaOutputFile") required () action { (x, c) ⇒
        c.copy(unmatchedDeltaOutputFile = x)
      } text "unmatchedDeltaOutputFile is a string property"
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

  def transform(spark: SparkSession, integratedContactPersons: Dataset[ContactPerson], dailyDeltaContactPersons: Dataset[ContactPerson]): (Dataset[ContactPerson], Dataset[ContactPerson]) = {
    import spark.implicits._

    // take over exact matches on concatId & copy ohubId (if we don't we might loose an ohubId here)
    val updatedContactPersons = integratedContactPersons
      .joinWith(dailyDeltaContactPersons, integratedContactPersons("concatId") === dailyDeltaContactPersons("concatId"), JoinType.Inner)
      .map { case (integratedCP, deltaCP) ⇒ deltaCP.copy(ohubId = integratedCP.ohubId) } // TODO can we set the ohubId regardless what has changed, what about dates? we might loose info here, is that ok?

    val matchedExact: Dataset[ContactPerson] = determineExactMatches(spark, integratedContactPersons, dailyDeltaContactPersons)

    val updatedIntegrated = integratedContactPersons
      .join(updatedContactPersons, Seq("concatId"), JoinType.LeftAnti) // integrated - updated
      .as[ContactPerson]
      .union(updatedContactPersons)
      .union(matchedExact)

    val unmatchedContactPersons = dailyDeltaContactPersons
      .join(updatedContactPersons, Seq("concatId"), JoinType.LeftAnti)
      .join(matchedExact, Seq("concatId"), JoinType.LeftAnti)
      .as[ContactPerson]

    (updatedIntegrated, unmatchedContactPersons)
  }

  private def determineExactMatches(spark: SparkSession, integratedContactPersons: Dataset[ContactPerson], dailyDeltaContactPersons: Dataset[ContactPerson]): Dataset[ContactPerson] = {
    import spark.implicits._

    lazy val integratedWithExact = integratedContactPersons
      .filter('emailAddress.isNotNull || 'mobileNumber.isNotNull)
      .map(cpn ⇒ (cpn.emailAddress.getOrElse("") + cpn.mobileNumber.getOrElse(""), cpn))
      .toDF("group", "contactPerson")

    lazy val newWithExact =
      dailyDeltaContactPersons
        .join(integratedContactPersons, Seq("concatId"), JoinType.LeftAnti)
        .filter('emailAddress.isNotNull || 'mobileNumber.isNotNull)
        .as[ContactPerson]
        .map(cpn ⇒ (cpn.emailAddress.getOrElse("") + cpn.mobileNumber.getOrElse(""), cpn))
        .toDF("group", "contactPerson")

    // give these the corresponding ohubId & append new to matching group
    newWithExact
      .joinWith(integratedWithExact, newWithExact("group") === integratedWithExact("group") && integratedWithExact("contactPerson.isGoldenRecord"), JoinType.Inner) // an exact match with the integrated golden record
      .as[((String, ContactPerson), (String, ContactPerson))]
      .map { case ((_, newDeltaExact), (_, integratedExact)) ⇒ newDeltaExact.copy(ohubId = integratedExact.ohubId) } // group exact match with integrated
      .as[ContactPerson]
  }

  override def run(spark: SparkSession, config: ExactMatchIngestedWithDbConfig, storage: Storage): Unit = {
    import spark.implicits._

    log.info(s"Integrated vs ingested exact matching contact persons from [${config.integratedInputFile}] and " +
      s"[${config.deltaInputFile}] to matched output [${config.exactMatchOutputFile}] and unmatched delta output to [${config.unmatchedDeltaOutputFile}]")

    val integratedContactPersons = storage.readFromParquet[ContactPerson](config.integratedInputFile)
    val dailyDeltaContactPersons = storage.readFromParquet[ContactPerson](config.deltaInputFile)

    val (exactMatches, unmatched) = transform(spark, integratedContactPersons, dailyDeltaContactPersons)

    storage.writeToParquet(exactMatches, config.exactMatchOutputFile)
    storage.writeToParquet(unmatched, config.unmatchedDeltaOutputFile)
  }
}
