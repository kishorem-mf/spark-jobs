package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.domain.entity.{ ContactPerson, InvalidEmail }
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.{ SparkJob, SparkJobConfig }
import org.apache.spark.sql.{ Dataset, Encoder, Encoders, SparkSession }
import scopt.OptionParser

case class ContactPersonUpdateEmailValidFlagConfig(
    contactPersonsInputFile: String = "contactpersons-input-file",
    invalidEmailAddressesInputFile: String = "invalid-email-addresses-input-file",
    outputFile: String = "path-to-output-file"
) extends SparkJobConfig

object ContactPersonUpdateEmailValidFlag extends SparkJob[ContactPersonUpdateEmailValidFlagConfig] {
  override private[spark] def defaultConfig = ContactPersonUpdateEmailValidFlagConfig()

  def transform(
    spark: SparkSession,
    contactPersons: Dataset[ContactPerson],
    invalidEmails: Dataset[InvalidEmail]): Dataset[ContactPerson] = {
    import spark.implicits._

    contactPersons.joinWith(invalidEmails.dropDuplicates(), contactPersons("emailAddress") === invalidEmails("emailAddress"), "left").map {
      case (contactPerson: ContactPerson, _: InvalidEmail) ⇒ contactPerson.copy(isEmailAddressValid = Some(false))
      case (contactPerson: ContactPerson, _)               ⇒ contactPerson.copy(isEmailAddressValid = Some(true))
    }
  }

  override private[spark] def configParser(): OptionParser[ContactPersonUpdateEmailValidFlagConfig] =
    new scopt.OptionParser[ContactPersonUpdateEmailValidFlagConfig]("Contact person update validEmailFlag") {
      head("sets contact person the hasValidEmail flag based on invalidEmailAddressesInputFile", "1.0")
      opt[String]("contactPersonsInputFile") required () action { (x, c) ⇒
        c.copy(contactPersonsInputFile = x)
      } text "contactPersonsInputFile is a string property"
      opt[String]("invalidEmailAddressesInputFile") required () action { (x, c) ⇒
        c.copy(invalidEmailAddressesInputFile = x)
      } text "invalidEmailAddressesInputFile is a string property"
      opt[String]("outputFile") required () action { (x, c) ⇒
        c.copy(outputFile = x)
      } text "outputFile is a string property"

      version("1.0")
      help("help") text "help text"
    }

  override def run(spark: SparkSession, config: ContactPersonUpdateEmailValidFlagConfig, storage: Storage): Unit = {

    log.info(
      s"Set contact person valid email flag for [${config.contactPersonsInputFile}] " +
        s"based on [${config.invalidEmailAddressesInputFile}] " +
        s"and write result to [${config.outputFile}]"
    )

    val contactPersons = storage.readFromParquet[ContactPerson](config.contactPersonsInputFile)

    implicit val invalidEmailEncoder: Encoder[InvalidEmail] = Encoders.product[InvalidEmail]

    val invalidEmails = storage
      .readFromCsv(config.invalidEmailAddressesInputFile, ";")
      .select("emailAddress")
      .as[InvalidEmail]

    val transformed = transform(spark, contactPersons, invalidEmails)

    storage.writeToParquet(transformed, config.outputFile)
  }
}
