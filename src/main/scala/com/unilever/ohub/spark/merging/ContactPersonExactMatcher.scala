package com.unilever.ohub.spark.merging

import java.util.UUID

import com.unilever.ohub.spark.{ SparkJob, SparkJobConfig }
import com.unilever.ohub.spark.domain.entity.ContactPerson
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.tsv2parquet.DomainDataProvider
import org.apache.spark.sql.{ Dataset, SparkSession }
import org.apache.spark.sql.functions._
import scopt.OptionParser

case class ContactPersonExactMatchConfig(
    matchingInputFile: String = "matched-input-file",
    contactsInputFile: String = "contacts-input-file",
    outputFile: String = "path-to-output-file",
    postgressUrl: String = "postgress-url",
    postgressUsername: String = "postgress-username",
    postgressPassword: String = "postgress-password",
    postgressDB: String = "postgress-db"
) extends SparkJobConfig

object ContactPersonExactMatcher extends SparkJob[ContactPersonExactMatchConfig] with GoldenRecordPicking[ContactPerson] {

  def markGoldenRecordAndGroupId(sourcePreference: Map[String, Int])(contactPersons: Seq[ContactPerson]): Seq[ContactPerson] = {
    val goldenRecord = pickGoldenRecord(sourcePreference, contactPersons)
    val groupId = UUID.randomUUID().toString
    contactPersons.map(o ⇒ o.copy(ohubId = Some(groupId), isGoldenRecord = o == goldenRecord))
  }

  def transform(
    spark: SparkSession,
    matchedContactPersons: Dataset[ContactPerson],
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

  private[spark] def defaultConfig: ContactPersonExactMatchConfig = ContactPersonExactMatchConfig()

  private[spark] def configParser(): OptionParser[ContactPersonExactMatchConfig] =
    new scopt.OptionParser[ContactPersonExactMatchConfig]("Contact person merging") {
      head("merges contact persons from name matching and exact matching.", "1.0")
      opt[String]("matchingInputFile") required () action { (x, c) ⇒
        c.copy(matchingInputFile = x)
      } text "matchingInputFile is a string property"
      opt[String]("contactsInputFile") required () action { (x, c) ⇒
        c.copy(contactsInputFile = x)
      } text "contactsInputFile is a string property"
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

  override def run(spark: SparkSession, config: ContactPersonExactMatchConfig, storage: Storage): Unit = {
    run(spark, config, storage, DomainDataProvider(spark, config.postgressUrl, config.postgressDB, config.postgressUsername, config.postgressPassword))
  }

  protected[merging] def run(spark: SparkSession, config: ContactPersonExactMatchConfig, storage: Storage, dataProvider: DomainDataProvider): Unit = {
    import spark.implicits._

    log.info(s"Merging contact persons from [${config.matchingInputFile}] with [${config.contactsInputFile}] to [${config.outputFile}]")

    val matchedContactPersons = storage.readFromParquet[ContactPerson](config.matchingInputFile)
    val ingestedContactPersons = storage.readFromParquet[ContactPerson](config.contactsInputFile)
    val transformed = transform(spark, matchedContactPersons, ingestedContactPersons, dataProvider.sourcePreferences)

    storage.writeToParquet(transformed, config.outputFile)
  }
}
