package com.unilever.ohub.spark.acm

import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.data.GoldenContactPersonRecord
import com.unilever.ohub.spark.data.ufs.UFSRecipient
import com.unilever.ohub.spark.generic.StringFunctions
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{ Dataset, SparkSession }

object ContactPersonAcmConverter extends SparkJob {
  private val boolAsString = (bool: Boolean) => if (bool) "Y" else "N"
  private val clean = (str: String) => StringFunctions.removeGenericStrangeChars(str)
  private val cleanNames = (firstName: String, lastName: String, isFirstName: Boolean) => {
    StringFunctions.fillLastNameOnlyWhenFirstEqualsLastName(firstName, lastName, isFirstName)
  }
  private val dateFormat = "yyyy-MM-dd HH:mm:ss"

  def transform(
    spark: SparkSession,
    goldenContactPersonRecords: Dataset[GoldenContactPersonRecord]
  ): Dataset[UFSRecipient] = {
    import spark.implicits._

    goldenContactPersonRecords.map { goldenContactPersonRecord =>
      val contactPerson = goldenContactPersonRecord.contactPerson

      UFSRecipient(
        CP_ORIG_INTEGRATION_ID = contactPerson.contactPersonConcatId,
        CP_LNKD_INTEGRATION_ID = goldenContactPersonRecord.ohubContactPersonId,
        OPR_ORIG_INTEGRATION_ID = contactPerson.refOperatorId,
        GOLDEN_RECORD_FLAG = "Y",
        WEB_CONTACT_ID = "",
        EMAIL_OPTOUT = contactPerson.emailOptOut.map(boolAsString),
        PHONE_OPTOUT = contactPerson.telemarketingOptOut.map(boolAsString),
        FAX_OPTOUT = contactPerson.faxOptOut.map(boolAsString),
        MOBILE_OPTOUT = contactPerson.mobileOptOut.map(boolAsString),
        DM_OPTOUT = contactPerson.directMailOptOut.map(boolAsString),
        LAST_NAME = cleanNames(
          contactPerson.firstName.getOrElse(""),
          contactPerson.lastName.getOrElse(""),
          false
        ),
        FIRST_NAME = cleanNames(
          contactPerson.firstName.getOrElse(""),
          contactPerson.lastName.getOrElse(""),
          true
        ),
        MIDDLE_NAME = "",
        TITLE = contactPerson.title,
        GENDER = contactPerson.gender.map {
          case "M" => "1"
          case "F" => "2"
          case _   => "0"
        },
        LANGUAGE = contactPerson.languageKey,
        EMAIL_ADDRESS = contactPerson.emailAddress,
        MOBILE_PHONE_NUMBER = contactPerson.mobilePhoneNumber,
        PHONE_NUMBER = contactPerson.phoneNumber,
        FAX_NUMBER = contactPerson.faxNumber,
        STREET = contactPerson.street.map(clean),
        HOUSENUMBER =
          contactPerson.houseNumber.map(clean).getOrElse("") +
            " " +
            contactPerson.houseNumberExt.map(clean).getOrElse(""),
        ZIPCODE = contactPerson.zipCode.map(clean),
        CITY = contactPerson.city.map(clean),
        COUNTRY = contactPerson.country,
        DATE_CREATED = contactPerson.dateCreated.map(_.formatted(dateFormat)),
        DATE_UPDATED = contactPerson.dateModified.map(_.formatted(dateFormat)),
        DATE_OF_BIRTH = contactPerson.birthDate.map(_.formatted(dateFormat)),
        PREFERRED = contactPerson.preferredContact.map(boolAsString),
        ROLE = contactPerson.function,
        COUNTRY_CODE = contactPerson.countryCode,
        SCM = contactPerson.scm,
        DELETE_FLAG = contactPerson.status.map(status => if (status) "0" else "1"),
        KEY_DECISION_MAKER = contactPerson.keyDecisionMaker.map(boolAsString),
        OPT_IN = contactPerson.emailOptIn.map(boolAsString),
        OPT_IN_DATE = contactPerson.emailOptInDate.map(_.formatted(dateFormat)),
        CONFIRMED_OPT_IN = contactPerson.emailOptInConfirmed.map(boolAsString),
        CONFIRMED_OPT_IN_DATE = contactPerson.emailOptInConfirmedDate.map(_.formatted(dateFormat)),
        MOB_OPT_IN = contactPerson.mobileOptIn.map(boolAsString),
        MOB_OPT_IN_DATE = contactPerson.mobileOptInDate.map(_.formatted(dateFormat)),
        MOB_CONFIRMED_OPT_IN = contactPerson.mobileOptInConfirmed.map(boolAsString),
        MOB_CONFIRMED_OPT_IN_DATE = contactPerson.mobileOptInConfirmedDate.map(_.formatted(dateFormat)),
        MOB_OPT_OUT_DATE = "",
        ORG_FIRST_NAME = contactPerson.firstName,
        ORG_LAST_NAME = contactPerson.lastName,
        ORG_EMAIL_ADDRESS = contactPerson.emailAddressOriginal,
        ORG_FIXED_PHONE_NUMBER = contactPerson.phoneNumberOriginal,
        ORG_MOBILE_PHONE_NUMBER = contactPerson.mobilePhoneNumberOriginal,
        ORG_FAX_NUMBER = contactPerson.faxNumber
      )
    }
  }

  override val neededFilePaths = Array("INPUT_FILE", "OUTPUT_FILE")

  override def run(spark: SparkSession, filePaths: scala.Product, storage: Storage): Unit = {
    import spark.implicits._

    val (inputFile: String, outputFile: String) = filePaths

    log.info(s"Generating contact person ACM csv file from [$inputFile] to [$outputFile]")

    val goldenContactPersonRecords = storage
      .readFromParquet[GoldenContactPersonRecord](inputFile)

    val transformed = transform(spark, goldenContactPersonRecords)

    storage
      .writeToCsv(transformed, outputFile, partitionBy = Seq("COUNTRY_CODE"))
  }
}
