package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.domain.entity.{ContactPerson, InvalidMobile}
import com.unilever.ohub.spark.sql.JoinType
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.{SparkJob, SparkJobConfig}
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
import scopt.OptionParser

case class ContactPersonUpdateMobileValidConfig (
     contactPersonsInputFile: String = "contactpersons-input-file",
     invalidMobileNumberInputFile: String = "invalid-mobile-input-file",
     outputFile: String = "contactpersons-output-file"
) extends SparkJobConfig

object ContactPersonUpdateMobileValidFlag extends SparkJob[ContactPersonUpdateMobileValidConfig ] {

  override private[spark] def defaultConfig = ContactPersonUpdateMobileValidConfig()

  override private[spark] def configParser(): OptionParser[ContactPersonUpdateMobileValidConfig] = {
    new scopt.OptionParser[ContactPersonUpdateMobileValidConfig]("Contact person update validMobileFlag") {
      head("sets contact person the hasValidMobile flag based on invalidMobileNumberInputFile", "1.0")
      opt[String]("contactPersonsInputFile") required() action { (x, c) ⇒
        c.copy(contactPersonsInputFile = x)
      } text "contactPersonsInputFile is a string property"
      opt[String]("invalidMobileNumberInputFile") required() action { (x, c) ⇒
        c.copy(invalidMobileNumberInputFile = x)
      } text "invalidMobileNumberInputFile is a string property"
      opt[String]("outputFile") required() action { (x, c) ⇒
        c.copy(outputFile = x)
      } text "outputFile is a string property"

      version("1.0")
      help("help") text "help text"

    }
  }

  def transform(
                 spark: SparkSession,
                 contactPersons: Dataset[ContactPerson],
                 invalidMobiles: Dataset[InvalidMobile]): Dataset[ContactPerson] = {
    import spark.implicits._

    contactPersons.joinWith(invalidMobiles.dropDuplicates(), contactPersons("mobileNumber") === invalidMobiles("mobileNumber"), JoinType.Left).map {
      case(contactPerson: ContactPerson, _: InvalidMobile) => contactPerson.copy(isMobileNumberValid = Some(false))
      case(contactPerson: ContactPerson, _) => contactPerson.copy(isMobileNumberValid = Some(true))
    }
  }

  override def run(spark: SparkSession, config: ContactPersonUpdateMobileValidConfig, storage: Storage): Unit = {

    log.info(
      s"Set contact person valid mobile flag for [${config.contactPersonsInputFile}] " +
        s"based on [${config.invalidMobileNumberInputFile}] " +
        s"and write result to [${config.outputFile}]"
    )

    val contactPersonsInput = storage.readFromParquet[ContactPerson](config.contactPersonsInputFile)

    implicit val invalidMobileEncoder: Encoder[InvalidMobile] = Encoders.product[InvalidMobile]

    val invalidMobiles= storage
      .readFromCsv(config.invalidMobileNumberInputFile, ";")
      .withColumnRenamed("ohubMobileNumber", "mobileNumber")
      .select("mobileNumber")
      .as[InvalidMobile]

    val updatedValidMobileNumberDS = transform(spark, contactPersonsInput, invalidMobiles)

    storage.writeToParquet(updatedValidMobileNumberDS, config.outputFile)
  }
}
