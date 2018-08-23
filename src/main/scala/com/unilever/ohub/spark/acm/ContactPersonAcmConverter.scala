package com.unilever.ohub.spark.acm

import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.export.DeltaFunctions
import com.unilever.ohub.spark.acm.model.AcmContactPerson
import com.unilever.ohub.spark.domain.entity.ContactPerson
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.DomainDataProvider
import org.apache.spark.sql.{ Dataset, SparkSession }
import scopt.OptionParser

object ContactPersonAcmConverter extends SparkJob[DefaultWithDeltaConfig]
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

  override private[spark] def defaultConfig = DefaultWithDeltaConfig()

  override private[spark] def configParser(): OptionParser[DefaultWithDeltaConfig] = DefaultWithDeltaConfigParser()

  override def run(spark: SparkSession, config: DefaultWithDeltaConfig, storage: Storage): Unit = {
    import spark.implicits._

    log.info(s"Generating contact person ACM csv file from [$config.inputFile] to [$config.outputFile]")

    val dataProvider = DomainDataProvider(spark)

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

    contactPersons.filter(_.isGoldenRecord).map { cp ⇒
      // TODO check whether the filter is at the right location
      cp match {
        case ContactPerson(concatId, countryCode, customerType, dateCreated, dateUpdated, isActive, isGoldenRecord, ohubId, name, sourceEntityId, sourceName, ohubCreated, ohubUpdated, operatorConcatId, operatorOhubId, oldIntegrationId, firstName, lastName, title, gender, jobTitle, language, birthDate, street, houseNumber, houseNumberExtension, city, zipCode, state, countryName, isPreferredContact, isKeyDecisionMaker, standardCommunicationChannel, emailAddress, phoneNumber, mobileNumber, faxNumber, hasGeneralOptOut, hasConfirmedRegistration, confirmedRegistrationDate, hasEmailOptIn, emailOptInDate, hasEmailDoubleOptIn, emailDoubleOptInDate, hasEmailOptOut, hasDirectMailOptIn, hasDirectMailOptOut, hasTeleMarketingOptIn, hasTeleMarketingOptOut, hasMobileOptIn, mobileOptInDate, hasMobileDoubleOptIn, mobileDoubleOptInDate, hasMobileOptOut, hasFaxOptIn, hasFaxOptOut, webUpdaterId, additionalFields, ingestionErrors) ⇒ AcmContactPerson(
          CP_ORIG_INTEGRATION_ID = concatId,
          CP_LNKD_INTEGRATION_ID = ohubId.get, // TODO resolve .get here...what if we don't have an ohubId?
          OPR_ORIG_INTEGRATION_ID = oldIntegrationId.get, // TODO opr-ohub-id...add to domain (is set in the merging step)
          GOLDEN_RECORD_FLAG = "Y",
          WEB_CONTACT_ID = Option.empty,
          EMAIL_OPTOUT = hasEmailOptOut.map(boolAsString),
          PHONE_OPTOUT = hasTeleMarketingOptOut.map(boolAsString),
          FAX_OPTOUT = hasFaxOptOut.map(boolAsString),
          MOBILE_OPTOUT = hasMobileOptOut.map(boolAsString),
          DM_OPTOUT = hasDirectMailOptOut.map(boolAsString),
          LAST_NAME = lastName,
          FIRST_NAME = Option(cleanNames(
            firstName.getOrElse(""),
            lastName.getOrElse("")
          )),
          MIDDLE_NAME = Option.empty,
          TITLE = title,
          GENDER = gender.map {
            case "M" ⇒ "1"
            case "F" ⇒ "2"
            case _   ⇒ "0"
          },
          LANGUAGE = language,
          EMAIL_ADDRESS = emailAddress,
          MOBILE_PHONE_NUMBER = mobileNumber,
          PHONE_NUMBER = phoneNumber,
          FAX_NUMBER = faxNumber,
          STREET = street.map(clean),
          HOUSENUMBER = Option(Seq(houseNumber, houseNumberExtension).flatten.map(clean).mkString(" ")),
          ZIPCODE = zipCode.map(clean),
          CITY = city.map(clean),
          COUNTRY = countryName,
          DATE_CREATED = dateCreated.map(formatWithPattern()),
          DATE_UPDATED = dateUpdated.map(formatWithPattern()),
          DATE_OF_BIRTH = birthDate.map(formatWithPattern()),
          PREFERRED = isPreferredContact.map(boolAsString),
          ROLE = jobTitle,
          COUNTRY_CODE = countryCode,
          SCM = standardCommunicationChannel,
          DELETE_FLAG = if (isActive) "0" else "1",
          KEY_DECISION_MAKER = isKeyDecisionMaker.map(boolAsString),
          OPT_IN = hasEmailOptIn.map(boolAsString),
          OPT_IN_DATE = emailOptInDate.map(formatWithPattern()),
          CONFIRMED_OPT_IN = hasConfirmedRegistration.map(boolAsString),
          CONFIRMED_OPT_IN_DATE = confirmedRegistrationDate.map(formatWithPattern()),
          MOB_OPT_IN = hasMobileOptIn.map(boolAsString),
          MOB_OPT_IN_DATE = mobileOptInDate.map(formatWithPattern()),
          MOB_CONFIRMED_OPT_IN = hasMobileDoubleOptIn.map(boolAsString),
          MOB_CONFIRMED_OPT_IN_DATE = mobileDoubleOptInDate.map(formatWithPattern()),
          MOB_OPT_OUT_DATE = Option.empty,
          ORG_FIRST_NAME = firstName,
          ORG_LAST_NAME = lastName,
          ORG_EMAIL_ADDRESS = emailAddress,
          ORG_FIXED_PHONE_NUMBER = phoneNumber,
          ORG_MOBILE_PHONE_NUMBER = mobileNumber,
          ORG_FAX_NUMBER = faxNumber
        )
      }
    }
  }

}
