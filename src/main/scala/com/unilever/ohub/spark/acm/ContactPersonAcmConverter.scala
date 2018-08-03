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

    contactPersons.filter(_.isGoldenRecord).map { cp ⇒ // TODO check whether the filter is at the right location
      AcmContactPerson(
        CP_ORIG_INTEGRATION_ID = cp.concatId,
        CP_LNKD_INTEGRATION_ID = cp.ohubId.getOrElse(""), // TODO what if we don't have an ohubId?
        OPR_ORIG_INTEGRATION_ID = cp.oldIntegrationId.getOrElse(""), // TODO opr-ohub-id...add to domain (is set in the merging step)
        GOLDEN_RECORD_FLAG = "Y",
        WEB_CONTACT_ID = Option.empty,
        EMAIL_OPTOUT = cp.hasEmailOptOut.map(boolAsString),
        PHONE_OPTOUT = cp.hasTeleMarketingOptOut.map(boolAsString),
        FAX_OPTOUT = cp.hasFaxOptOut.map(boolAsString),
        MOBILE_OPTOUT = cp.hasMobileOptOut.map(boolAsString),
        DM_OPTOUT = cp.hasDirectMailOptOut.map(boolAsString),
        LAST_NAME = cp.lastName,
        FIRST_NAME = Option(cleanNames(
          cp.firstName.getOrElse(""),
          cp.lastName.getOrElse("")
        )),
        MIDDLE_NAME = Option.empty,
        TITLE = cp.title,
        GENDER = cp.gender.map {
          case "M" ⇒ "1"
          case "F" ⇒ "2"
          case _   ⇒ "0"
        },
        LANGUAGE = cp.language,
        EMAIL_ADDRESS = cp.emailAddress,
        MOBILE_PHONE_NUMBER = cp.mobileNumber,
        PHONE_NUMBER = cp.phoneNumber,
        FAX_NUMBER = cp.faxNumber,
        STREET = cp.street.map(clean),
        HOUSENUMBER = Option(Seq(cp.houseNumber, cp.houseNumberExtension).flatten.map(clean).mkString(" ")),
        ZIPCODE = cp.zipCode.map(clean),
        CITY = cp.city.map(clean),
        COUNTRY = cp.countryName,
        DATE_CREATED = cp.dateCreated.map(formatWithPattern()),
        DATE_UPDATED = cp.dateUpdated.map(formatWithPattern()),
        DATE_OF_BIRTH = cp.birthDate.map(formatWithPattern()),
        PREFERRED = cp.isPreferredContact.map(boolAsString),
        ROLE = cp.jobTitle,
        COUNTRY_CODE = cp.countryCode,
        SCM = cp.standardCommunicationChannel,
        DELETE_FLAG = if (cp.isActive) "0" else "1",
        KEY_DECISION_MAKER = cp.isKeyDecisionMaker.map(boolAsString),
        OPT_IN = cp.hasEmailOptIn.map(boolAsString),
        OPT_IN_DATE = cp.emailOptInDate.map(formatWithPattern()),
        CONFIRMED_OPT_IN = cp.hasConfirmedRegistration.map(boolAsString),
        CONFIRMED_OPT_IN_DATE = cp.confirmedRegistrationDate.map(formatWithPattern()),
        MOB_OPT_IN = cp.hasMobileOptIn.map(boolAsString),
        MOB_OPT_IN_DATE = cp.mobileOptInDate.map(formatWithPattern()),
        MOB_CONFIRMED_OPT_IN = cp.hasMobileDoubleOptIn.map(boolAsString),
        MOB_CONFIRMED_OPT_IN_DATE = cp.mobileDoubleOptInDate.map(formatWithPattern()),
        MOB_OPT_OUT_DATE = Option.empty,
        ORG_FIRST_NAME = cp.firstName,
        ORG_LAST_NAME = cp.lastName,
        ORG_EMAIL_ADDRESS = cp.emailAddress,
        ORG_FIXED_PHONE_NUMBER = cp.phoneNumber,
        ORG_MOBILE_PHONE_NUMBER = cp.mobileNumber,
        ORG_FAX_NUMBER = cp.faxNumber
      )
    }
  }

}
