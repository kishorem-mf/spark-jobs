package com.unilever.ohub.spark.acm

import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.acm.model.UFSRecipient
import com.unilever.ohub.spark.domain.entity.ContactPerson
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{ Dataset, SparkSession }

object ContactPersonAcmConverter extends SparkJob with AcmTransformationFunctions {

  def transform(spark: SparkSession, contactPersons: Dataset[ContactPerson]): Dataset[UFSRecipient] = {
    import spark.implicits._

    contactPersons.map { contactPerson ⇒

      UFSRecipient(
        CP_ORIG_INTEGRATION_ID = contactPerson.concatId,
        CP_LNKD_INTEGRATION_ID = contactPerson.ohubId.get, // TODO resolve .get here...what if we don't have an ohubId?
        OPR_ORIG_INTEGRATION_ID = contactPerson.oldIntegrationId,
        GOLDEN_RECORD_FLAG = "Y", // TODO do we need to filter here (or somewhere else)?
        WEB_CONTACT_ID = "",
        EMAIL_OPTOUT = contactPerson.hasEmailOptOut.map(boolAsString),
        PHONE_OPTOUT = contactPerson.hasTeleMarketingOptOut.map(boolAsString),
        FAX_OPTOUT = contactPerson.hasFaxOptOut.map(boolAsString),
        MOBILE_OPTOUT = contactPerson.hasMobileOptOut.map(boolAsString),
        DM_OPTOUT = contactPerson.hasDirectMailOptOut.map(boolAsString),
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
          case "M" ⇒ "1"
          case "F" ⇒ "2"
          case _   ⇒ "0"
        },
        LANGUAGE = contactPerson.language,
        EMAIL_ADDRESS = contactPerson.emailAddress,
        MOBILE_PHONE_NUMBER = contactPerson.mobileNumber,
        PHONE_NUMBER = contactPerson.phoneNumber,
        FAX_NUMBER = contactPerson.faxNumber,
        STREET = contactPerson.street.map(clean),
        HOUSENUMBER =
          contactPerson.houseNumber.map(clean).getOrElse("") +
            " " +
            contactPerson.houseNumberExtension.map(clean).getOrElse(""),
        ZIPCODE = contactPerson.zipCode.map(clean),
        CITY = contactPerson.city.map(clean),
        COUNTRY = contactPerson.countryName,
        DATE_CREATED = contactPerson.dateCreated.map(formatWithPattern()),
        DATE_UPDATED = contactPerson.dateUpdated.map(formatWithPattern()),
        DATE_OF_BIRTH = contactPerson.birthDate.map(formatWithPattern()),
        PREFERRED = contactPerson.isPreferredContact.map(boolAsString),
        ROLE = contactPerson.jobTitle,
        COUNTRY_CODE = Some(contactPerson.countryCode),
        SCM = contactPerson.standardCommunicationChannel,
        DELETE_FLAG = if (contactPerson.isActive) Some("0") else Some("1"), // TODO verify
        KEY_DECISION_MAKER = contactPerson.isKeyDecisionMaker.map(boolAsString),
        OPT_IN = contactPerson.hasEmailOptIn.map(boolAsString),
        OPT_IN_DATE = contactPerson.emailOptInDate.map(formatWithPattern()),
        CONFIRMED_OPT_IN = contactPerson.hasConfirmedRegistration.map(boolAsString),
        CONFIRMED_OPT_IN_DATE = contactPerson.confirmedRegistrationDate.map(formatWithPattern()),
        MOB_OPT_IN = contactPerson.hasMobileOptIn.map(boolAsString),
        MOB_OPT_IN_DATE = contactPerson.mobileOptInDate.map(formatWithPattern()),
        MOB_CONFIRMED_OPT_IN = contactPerson.hasMobileDoubleOptIn.map(boolAsString),
        MOB_CONFIRMED_OPT_IN_DATE = contactPerson.mobileDoubleOptInDate.map(formatWithPattern()),
        MOB_OPT_OUT_DATE = "",
        ORG_FIRST_NAME = contactPerson.firstName,
        ORG_LAST_NAME = contactPerson.lastName,
        ORG_EMAIL_ADDRESS = contactPerson.emailAddress,
        ORG_FIXED_PHONE_NUMBER = contactPerson.phoneNumber,
        ORG_MOBILE_PHONE_NUMBER = contactPerson.mobileNumber,
        ORG_FAX_NUMBER = contactPerson.faxNumber
      )
    }
  }

  override val neededFilePaths = Array("INPUT_FILE", "OUTPUT_FILE")

  override def run(spark: SparkSession, filePaths: scala.Product, storage: Storage): Unit = {
    import spark.implicits._

    val (inputFile: String, outputFile: String) = filePaths
    log.info(s"Generating contact person ACM csv file from [$inputFile] to [$outputFile]")

    val contactPersons = storage.readFromParquet[ContactPerson](inputFile)
    val transformed = transform(spark, contactPersons)

    storage.writeToCsv(transformed, outputFile, partitionBy = Seq("COUNTRY_CODE"))
  }
}
