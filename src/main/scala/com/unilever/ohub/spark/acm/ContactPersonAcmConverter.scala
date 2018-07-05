package com.unilever.ohub.spark.acm

import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.export.DeltaFunctions
import com.unilever.ohub.spark.acm.model.AcmContactPerson
import com.unilever.ohub.spark.domain.entity.ContactPerson
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.DomainDataProvider
import org.apache.spark.sql.{ Dataset, SparkSession }
import scopt.OptionParser

object ContactPersonAcmConverter extends SparkJob[DefaultWithDbAndDeltaConfig]
  with DeltaFunctions with AcmTransformationFunctions with AcmConverter {

  def transform(
    spark: SparkSession,
    contactPersons: Dataset[ContactPerson],
    previousIntegrated: Dataset[ContactPerson]
  ): Dataset[AcmContactPerson] = {
    val dailyAcmContactPersons = createAcmContactPersons(spark, contactPersons)
    val allPreviousAcmContactPersons = createAcmContactPersons(spark, previousIntegrated)

    integrate[AcmContactPerson](spark, dailyAcmContactPersons, allPreviousAcmContactPersons, "CP_ORIG_INTEGRATION_ID")
  }

  override private[spark] def defaultConfig = DefaultWithDbAndDeltaConfig()

  override private[spark] def configParser(): OptionParser[DefaultWithDbAndDeltaConfig] = DefaultWithDbAndDeltaConfigParser()

  override def run(spark: SparkSession, config: DefaultWithDbAndDeltaConfig, storage: Storage): Unit = {
    import spark.implicits._

    log.info(s"Generating contact person ACM csv file from [$config.inputFile] to [$config.outputFile]")

    val dataProvider = DomainDataProvider(spark, config.postgressUrl, config.postgressDB, config.postgressUsername, config.postgressPassword)

    val contactPersons = storage.readFromParquet[ContactPerson](config.inputFile)
    val previousIntegrated = config.previousIntegrated match {
      case Some(s) ⇒ storage.readFromParquet[ContactPerson](s)
      case None ⇒
        log.warn(s"No existing integrated file specified -- regarding as initial load.")
        spark.emptyDataset[ContactPerson]
    }
    val transformed = transform(spark, contactPersons, previousIntegrated)

    storage.writeToSingleCsv(
      ds = transformed,
      outputFile = config.outputFile,
      options = extraWriteOptions
    )
  }

  def createAcmContactPersons(
    spark: SparkSession,
    contactPersons: Dataset[ContactPerson]
  ): Dataset[AcmContactPerson] = {
    import spark.implicits._

    contactPersons.filter(_.isGoldenRecord).map { contactPerson ⇒ // TODO check whether the filter is at the right location
      AcmContactPerson(
        CP_ORIG_INTEGRATION_ID = contactPerson.concatId,
        CP_LNKD_INTEGRATION_ID = contactPerson.ohubId.get, // TODO resolve .get here...what if we don't have an ohubId?
        OPR_ORIG_INTEGRATION_ID = contactPerson.oldIntegrationId, // TODO opr-ohub-id...add to domain (is set in the merging step)
        GOLDEN_RECORD_FLAG = "Y",
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
        DELETE_FLAG = if (contactPerson.isActive) Some("0") else Some("1"),
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

}
