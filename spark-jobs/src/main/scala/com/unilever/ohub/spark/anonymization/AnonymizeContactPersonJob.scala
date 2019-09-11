package com.unilever.ohub.spark.anonymization

import com.unilever.ohub.spark.domain.entity.ContactPerson
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.{ SparkJob, SparkJobConfig }
import org.apache.spark.sql.{ Column, Dataset, SparkSession }
import scopt.OptionParser

case class PersonalInformationConfig(
    contactPersonsInputFile: String = "contactpersons-input-file",
    maskPersonalDataInputFile: String = "file-with-which-cp-to-mask",
    outputFile: String = "path-to-output-file"
) extends SparkJobConfig

object AnonymizeContactPersonJob extends SparkJob[PersonalInformationConfig] {

  val hiddenValue = Some("HIDDEN")

  override private[spark] def defaultConfig = PersonalInformationConfig()

  def transform(
    contactPersons: Dataset[ContactPerson],
    maskedContactPersons: Dataset[AnonymizedContactPersonIdentifier])(implicit spark: SparkSession): Dataset[ContactPerson] = {
    import org.apache.spark.sql.functions._
    import spark.implicits._

    val numBits = 256

    val extendedCP = contactPersons
      .withColumn("hashedEmailAddress", upper(sha2(col("emailAddress"), numBits)))
      .withColumn("hashedMobileNumber", upper(sha2(col("mobileNumber"), numBits)))
      .as[ContactPerson]
    extendedCP
      .joinWith(maskedContactPersons, extendedCP("hashedEmailAddress") === maskedContactPersons("hashedEmailAddress") ||
        extendedCP("hashedMobileNumber") === maskedContactPersons("hashedMobileNumber"), "left")
      .map {
        case (cp: ContactPerson, _: AnonymizedContactPersonIdentifier) ⇒
          cp.copy(firstName = hiddenValue, lastName = hiddenValue, emailAddress = hiddenValue, phoneNumber = hiddenValue, mobileNumber = hiddenValue, zipCode = hiddenValue)
        case (cp: ContactPerson, _) ⇒ cp
      }.drop("hashedEmailAddress", "hashedMobileNumber")
      .as[ContactPerson]
  }

  override private[spark] def configParser(): OptionParser[PersonalInformationConfig] =
    new scopt.OptionParser[PersonalInformationConfig]("Contact person hide personal information") {
      head("Once hidden always stay hidden", "1.0")
      opt[String]("contactPersonsInputFile") required () action { (x, c) ⇒
        c.copy(contactPersonsInputFile = x)
      } text "contactPersonsInputFile is a string property"
      opt[String]("maskPersonalDataInputFile") required () action { (x, c) ⇒
        c.copy(maskPersonalDataInputFile = x)
      } text "inputFile is a string property"
      opt[String]("outputFile") required () action { (x, c) ⇒
        c.copy(outputFile = x)
      } text "outputFile is a string property"

      version("1.0")
      help("help") text "help text"
    }

  override def run(spark: SparkSession, config: PersonalInformationConfig, storage: Storage): Unit = {
    val contactPersons = storage.readFromParquet[ContactPerson](config.contactPersonsInputFile)
    val maskedContactPersons = storage.readFromParquet[AnonymizedContactPersonIdentifier](config.maskPersonalDataInputFile)

    implicit val implicitSpark: SparkSession = spark
    val transformed = transform(contactPersons, maskedContactPersons)

    storage.writeToParquet(transformed, config.outputFile)
  }
}

