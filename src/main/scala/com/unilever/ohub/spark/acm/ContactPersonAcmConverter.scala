package com.unilever.ohub.spark.acm

import java.sql.Timestamp

import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.generic.StringFunctions
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{ Dataset, SparkSession }

case class Recipient(
  CONTACT_PERSON_CONCAT_ID: String,
  OHUB_CONTACT_PERSON_ID: String,
  REF_OPERATOR_ID: String,
  EM_OPT_OUT: Boolean,
  TM_OPT_IN: Boolean,
  FAX_OPT_OUT: Boolean,
  MOB_OPT_OUT: Boolean,
  DM_OPT_OUT: Boolean,
  TITLE: String,
  GENDER: String,
  LANGUAGE_KEY: String,
  EMAIL_ADDRESS: String,
  MOBILE_PHONE_NUMBER: String,
  PHONE_NUMBER: String,
  FAX_NUMBER: String,
  STREET: String,
  HOUSENUMBER: String,
  HOUSENUMBER_EXT: String,
  ZIP_CODE: String,
  CITY: String,
  COUNTRY: String,
  DATE_CREATED: Timestamp,
  DATE_MODIFIED: Timestamp,
  BIRTH_DATE: Timestamp,
  PREFERRED_CONTACT: Boolean,
  FUNCTION: String,
  COUNTRY_CODE: String,
  SCM: String,
  STATUS: Boolean,
  KEY_DECISION_MAKER: Boolean,
  EM_OPT_IN: Boolean,
  EM_OPT_IN_DATE: Timestamp,
  EM_OPT_IN_CONFIRMED: Boolean,
  EM_OPT_IN_CONFIRMED_DATE: Timestamp,
  MOB_OPT_IN: Boolean,
  MOB_OPT_IN_DATE: Timestamp,
  MOB_OPT_IN_CONFIRMED: Boolean,
  MOB_OPT_IN_CONFIRMED_DATE: Timestamp,
  FIRST_NAME: String,
  LAST_NAME: String,
  EMAIL_ADDRESS_ORIGINAL: String,
  PHONE_NUMBER_ORIGINAL: String
)

case class OHubContactPersonIdAndRecipients(OHUB_CONTACT_PERSON_ID: String, CONTACT_PERSON: Seq[Recipient])

// Data Model: OPR_ORIG_INTEGRATION_ID can be misleading for Ohub 2.0
// as this will contain the new OHUB_OPERATOR_ID and OPR_LNKD_INTEGRATION_ID will contain OPERATOR_CONCAT_ID
case class UfsRecipient(
  CP_ORIG_INTEGRATION_ID: String,
  CP_LNKD_INTEGRATION_ID: String,
  OPR_ORIG_INTEGRATION_ID: String,
  GOLDEN_RECORD_FLAG: String,
  WEB_CONTACT_ID: String,
  EMAIL_OPTOUT: String,
  PHONE_OPTOUT: String,
  FAX_OPTOUT: String,
  MOBILE_OPTOUT: String,
  DM_OPTOUT: String,
  LAST_NAME: String,
  FIRST_NAME: String,
  MIDDLE_NAME: String,
  TITLE: String,
  GENDER: String,
  LANGUAGE: String,
  EMAIL_ADDRESS: String,
  MOBILE_PHONE_NUMBER: String,
  PHONE_NUMBER: String,
  FAX_NUMBER: String,
  STREET: String,
  HOUSENUMBER: String,
  ZIPCODE: String,
  CITY: String,
  COUNTRY: String,
  DATE_CREATED: String,
  DATE_UPDATED: String,
  DATE_OF_BIRTH: String,
  PREFERRED: String,
  ROLE: String,
  COUNTRY_CODE: String,
  SCM: String,
  DELETE_FLAG: String,
  KEY_DECISION_MAKER: String,
  OPT_IN: String,
  OPT_IN_DATE: String,
  CONFIRMED_OPT_IN: String,
  CONFIRMED_OPT_IN_DATE: String,
  MOB_OPT_IN: String,
  MOB_OPT_IN_DATE: String,
  MOB_CONFIRMED_OPT_IN: String,
  MOB_CONFIRMED_OPT_IN_DATE: String,
  MOB_OPT_OUT_DATE: String,
  ORG_FIRST_NAME: String,
  ORG_LAST_NAME: String,
  ORG_EMAIL_ADDRESS: String,
  ORG_FIXED_PHONE_NUMBER: String,
  ORG_MOBILE_PHONE_NUMBER: String,
  ORG_FAX_NUMBER: String
)

object ContactPersonAcmConverter extends SparkJob {
  def transform(
    spark: SparkSession,
    contactPersonIdAndRecipients: Dataset[OHubContactPersonIdAndRecipients]
  ): Dataset[UfsRecipient] = {
    import spark.implicits._

    val boolAsString = (bool: Boolean) => if (bool) "Y" else "N"
    val clean = (str: String) => StringFunctions.removeGenericStrangeChars(str)
    val cleanNames = (firstName: String, lastName: String, isFirstName: Boolean) => {
      StringFunctions.fillLastNameOnlyWhenFirstEqualsLastName(firstName, lastName, isFirstName)
    }
    val dateFormat = "yyyy-MM-dd HH:mm:ss"

    contactPersonIdAndRecipients.flatMap(_.CONTACT_PERSON.map(recipient => UfsRecipient(
      CP_ORIG_INTEGRATION_ID = recipient.CONTACT_PERSON_CONCAT_ID,
      CP_LNKD_INTEGRATION_ID = recipient.OHUB_CONTACT_PERSON_ID,
      OPR_ORIG_INTEGRATION_ID = recipient.REF_OPERATOR_ID,
      GOLDEN_RECORD_FLAG = "Y",
      WEB_CONTACT_ID = "",
      EMAIL_OPTOUT = boolAsString(recipient.EM_OPT_OUT),
      PHONE_OPTOUT = if (recipient.TM_OPT_IN) "N" else "Y",
      FAX_OPTOUT = boolAsString(recipient.FAX_OPT_OUT),
      MOBILE_OPTOUT = boolAsString(recipient.MOB_OPT_OUT),
      DM_OPTOUT = boolAsString(recipient.DM_OPT_OUT),
      LAST_NAME = cleanNames(recipient.FIRST_NAME, recipient.LAST_NAME, false),
      FIRST_NAME = cleanNames(recipient.FIRST_NAME, recipient.LAST_NAME, true),
      MIDDLE_NAME = "",
      TITLE = recipient.TITLE,
      GENDER = recipient.GENDER match {
        case "M" => "1"
        case "F" => "2"
        case _ => "0"
      },
      LANGUAGE = recipient.LANGUAGE_KEY,
      EMAIL_ADDRESS = recipient.EMAIL_ADDRESS,
      MOBILE_PHONE_NUMBER = recipient.MOBILE_PHONE_NUMBER,
      PHONE_NUMBER = recipient.PHONE_NUMBER,
      FAX_NUMBER = recipient.FAX_NUMBER,
      STREET = clean(recipient.STREET),
      HOUSENUMBER = clean(recipient.HOUSENUMBER) + " " + clean(recipient.HOUSENUMBER_EXT),
      ZIPCODE = clean(recipient.ZIP_CODE),
      CITY = clean(recipient.CITY),
      COUNTRY = recipient.COUNTRY,
      DATE_CREATED = recipient.DATE_CREATED.formatted(dateFormat),
      DATE_UPDATED = recipient.DATE_MODIFIED.formatted(dateFormat),
      DATE_OF_BIRTH = recipient.BIRTH_DATE.formatted(dateFormat),
      PREFERRED = boolAsString(recipient.PREFERRED_CONTACT),
      ROLE = recipient.FUNCTION,
      COUNTRY_CODE = recipient.COUNTRY_CODE,
      SCM = recipient.SCM,
      DELETE_FLAG = if (recipient.STATUS) "0" else "1",
      KEY_DECISION_MAKER = boolAsString(recipient.KEY_DECISION_MAKER),
      OPT_IN = boolAsString(recipient.EM_OPT_IN),
      OPT_IN_DATE = recipient.EM_OPT_IN_DATE.formatted(dateFormat),
      CONFIRMED_OPT_IN = boolAsString(recipient.EM_OPT_IN_CONFIRMED),
      CONFIRMED_OPT_IN_DATE = recipient.EM_OPT_IN_CONFIRMED_DATE.formatted(dateFormat),
      MOB_OPT_IN = boolAsString(recipient.MOB_OPT_IN),
      MOB_OPT_IN_DATE = recipient.MOB_OPT_IN_DATE.formatted(dateFormat),
      MOB_CONFIRMED_OPT_IN = boolAsString(recipient.MOB_OPT_IN_CONFIRMED),
      MOB_CONFIRMED_OPT_IN_DATE = recipient.MOB_OPT_IN_CONFIRMED_DATE.formatted(dateFormat),
      MOB_OPT_OUT_DATE = "",
      ORG_FIRST_NAME = recipient.FIRST_NAME,
      ORG_LAST_NAME = recipient.LAST_NAME,
      ORG_EMAIL_ADDRESS = recipient.EMAIL_ADDRESS_ORIGINAL,
      ORG_FIXED_PHONE_NUMBER = recipient.PHONE_NUMBER_ORIGINAL,
      ORG_MOBILE_PHONE_NUMBER = recipient.PHONE_NUMBER_ORIGINAL,
      ORG_FAX_NUMBER = recipient.FAX_NUMBER
    )))
  }

  override val neededFilePaths = Array("INPUT_FILE", "OUTPUT_FILE")

  override def run(spark: SparkSession, filePaths: scala.Product, storage: Storage): Unit = {
    import spark.implicits._

    val (inputFile: String, outputFile: String) = filePaths

    log.info(s"Generating contact person ACM csv file from [$inputFile] to [$outputFile]")

    val contactPersonIdAndRecipients = storage
      .readFromParquet[OHubContactPersonIdAndRecipients](
        inputFile,
        selectColumns = $"OHUB_CONTACT_PERSON_ID", $"CONTACT_PERSON.*"
      )

    val transformed = transform(spark, contactPersonIdAndRecipients)

    storage
      .writeToCSV(transformed, outputFile, partitionBy = "COUNTRY_CODE")
  }
}
