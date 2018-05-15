package com.unilever.ohub.spark.merging

import java.util.UUID

import com.unilever.ohub.spark.SparkJobConfig
import com.unilever.ohub.spark.domain.entity.ContactPerson
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.tsv2parquet.DomainDataProvider
import org.apache.spark.sql.SparkSession
import scopt.OptionParser

case class GroupObject(group: String, contactPerson: ContactPerson)

case class ContactPersonJoinConfig(
    matchingInputFile: String = "matched-input-file",
    contactsInputFile: String = "contacts-input-file",
    outputFile: String = "path-to-output-file",
    postgressUrl: String = "postgress-url",
    postgressUsername: String = "postgress-username",
    postgressPassword: String = "postgress-password",
    postgressDB: String = "postgress-db"
) extends SparkJobConfig

object ContactPersonMerging extends BaseMatchingJoiner[ContactPerson, ContactPersonJoinConfig] {

  def markGoldenRecordAndGroupId(sourcePreference: Map[String, Int])(contactPersons: Seq[ContactPerson]): Seq[ContactPerson] = {
    val goldenRecord = pickGoldenRecord(sourcePreference, contactPersons)
    val groupId = UUID.randomUUID().toString
    contactPersons.map(o ⇒ o.copy(ohubId = Some(groupId), isGoldenRecord = o == goldenRecord))
  }

  private[spark] def defaultConfig: ContactPersonJoinConfig = ContactPersonJoinConfig()

  private[spark] def configParser(): OptionParser[ContactPersonJoinConfig] =
    new scopt.OptionParser[ContactPersonJoinConfig]("Contact person merging") {
      head("joins contact persons from name matching and non exact matches.", "1.0")
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

  override def run(spark: SparkSession, config: ContactPersonJoinConfig, storage: Storage): Unit = {
    run(spark, config, storage, DomainDataProvider(spark, config.postgressUrl, config.postgressDB, config.postgressUsername, config.postgressPassword))
  }

  protected[merging] def run(spark: SparkSession, config: ContactPersonJoinConfig, storage: Storage, dataProvider: DomainDataProvider): Unit = {
    import spark.implicits._

    log.info(s"Joining contact persons from [${config.matchingInputFile}] with [${config.contactsInputFile}] to [${config.outputFile}]")

    val matchedContactPersons = storage.readFromParquet[MatchingResult](config.matchingInputFile)
    val allContactPersons = storage.readFromParquet[ContactPerson](config.contactsInputFile)
    val transformed = transform(spark, allContactPersons, matchedContactPersons, markGoldenRecordAndGroupId(dataProvider.sourcePreferences))

    storage.writeToParquet(transformed, config.outputFile)
  }
}
