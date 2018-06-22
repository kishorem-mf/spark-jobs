package com.unilever.ohub.spark.dispatcher

import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.export.DeltaFunctions
import com.unilever.ohub.spark.dispatcher.model.DispatcherContactPerson
import com.unilever.ohub.spark.domain.entity.ContactPerson
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{ Dataset, SparkSession }
import scopt.OptionParser

object ContactPersonDispatcherConverter extends SparkJob[DefaultWithDbAndDeltaConfig]
  with DeltaFunctions with DispatcherTransformationFunctions with DispatcherConverter {

  /**
   * A process that transforms a source [[com.unilever.ohub.spark.domain.entity.ContactPerson]] to a
   * destination [[com.unilever.ohub.spark.dispatcher.model.DispatcherContactPerson]] according to the following logic:
   * <ul>
   *   <li>Read the input file containing ContactPerson records</li>
   *   <li>Read an optional previous-run file containing ContactPerson records, and if present, integrate the input and previous file</li>
   *   <li>Transform the ContactPerson records to the [[com.unilever.ohub.spark.dispatcher.model.DispatcherContactPerson]]</li>
   * </ul>
   * @param spark EntryPoint to the Spark Platform
   * @param contactPersons Source [[com.unilever.ohub.spark.domain.entity.ContactPerson]]
   * @param previousIntegrated previous [[com.unilever.ohub.spark.domain.entity.ContactPerson]]
   * @return A Spark Dataset containing the transformed [[com.unilever.ohub.spark.dispatcher.model.DispatcherContactPerson]]
   */
  def transform(
    spark: SparkSession,
    contactPersons: Dataset[ContactPerson],
    previousIntegrated: Dataset[ContactPerson]
  ): Dataset[DispatcherContactPerson] = {
    val dailyUfsContactPersons = createUfsContactPersons(spark, contactPersons)
    val allPreviousUfsContactPersons = createUfsContactPersons(spark, previousIntegrated)

    integrate[DispatcherContactPerson](spark, dailyUfsContactPersons, allPreviousUfsContactPersons, "CP_ORIG_INTEGRATION_ID")
  }

  override private[spark] def defaultConfig = DefaultWithDbAndDeltaConfig()

  override private[spark] def configParser(): OptionParser[DefaultWithDbAndDeltaConfig] = DefaultWithDbAndDeltaConfigParser()

  override def run(spark: SparkSession, config: DefaultWithDbAndDeltaConfig, storage: Storage): Unit = {
    import spark.implicits._

    log.info(s"Generating contact person Dispatcher csv file from [$config.inputFile] to [$config.outputFile]")

    val contactPersons = storage.readFromParquet[ContactPerson](config.inputFile)
    val previousIntegrated = config.previousIntegrated match {
      case Some(s) ⇒ storage.readFromParquet[ContactPerson](s)
      case None ⇒
        log.warn(s"No existing integrated file specified -- regarding as initial load.")
        spark.emptyDataset[ContactPerson]
    }
    val transformed: Dataset[DispatcherContactPerson] = transform(spark, contactPersons, previousIntegrated)

    storage.writeToSingleCsv(
      ds = transformed,
      outputFile = config.outputFile,
      options = extraWriteOptions
    )
  }

  /**
   * Map a [[com.unilever.ohub.spark.domain.entity.ContactPerson]] to a [[com.unilever.ohub.spark.dispatcher.ContactPersonDispatcherConverter]]
   * @param spark EntryPoint to the Spark Platform
   * @param contactPersons A dataset of DispatcherContactPerson
   * @return A dataset of DispatcherContactPerson
   */
  def createUfsContactPersons(
    spark: SparkSession,
    contactPersons: Dataset[ContactPerson]
  ): Dataset[DispatcherContactPerson] = {
    import spark.implicits._

    contactPersons.filter(_.isGoldenRecord).map { cp ⇒
      DispatcherContactPerson(
        DATE_OF_BIRTH = cp.birthDate.mapWithDefaultPatternOpt,
        CITY = cp.city,
        CP_ORIG_INTEGRATION_ID = cp.concatId,
        COUNTRY_CODE = cp.countryCode,
        COUNTRY = cp.countryName,
        EMAIL_ADDRESS = cp.emailAddress,
        CONFIRMED_OPT_IN_DATE = cp.emailDoubleOptInDate.mapWithDefaultPatternOpt,
        OPT_IN_DATE = cp.emailOptInDate.mapWithDefaultPatternOpt,
        FAX_NUMBER = cp.faxNumber,
        GENDER = cp.gender,
        DM_OPT_OUT = cp.hasDirectMailOptOut,
        CONFIRMED_OPT_IN = cp.hasEmailDoubleOptIn,
        OPT_IN = cp.hasEmailOptIn,
        EMAIL_OPT_OUT = cp.hasEmailOptOut,
        FAX_OPT_OUT = cp.hasFaxOptOut,
        MOB_CONFIRMED_OPT_IN = cp.hasMobileDoubleOptIn,
        MOB_OPT_IN = cp.hasMobileOptIn,
        MOBILE_OPT_OUT = cp.hasMobileOptOut,
        FIXED_OPT_OUT = cp.hasTeleMarketingOptOut,
        HOUSE_NUMBER = cp.houseNumber,
        HOUSE_NUMBER_ADD = cp.houseNumberExtension,
        DELETE_FLAG = cp.isActive.invert,
        GOLDEN_RECORD_FLAG = cp.isGoldenRecord,
        KEY_DECISION_MAKER = cp.isKeyDecisionMaker,
        PREFERRED = cp.isPreferredContact,
        LANGUAGE = cp.language,
        LAST_NAME = cp.lastName,
        MOB_CONFIRMED_OPT_IN_DATE = cp.mobileDoubleOptInDate.mapWithDefaultPatternOpt,
        MOBILE_PHONE_NUMBER = cp.mobileNumber,
        MOB_OPT_IN_DATE = cp.mobileOptInDate.mapWithDefaultPatternOpt,
        CREATED_AT = cp.ohubCreated.mapWithDefaultPattern,
        CP_LNKD_INTEGRATION_ID = cp.ohubId,
        UPDATED_AT = cp.ohubUpdated.mapWithDefaultPattern,
        OPR_ORIG_INTEGRATION_ID = cp.operatorConcatId,
        FIXED_PHONE_NUMBER = cp.phoneNumber,
        SOURCE_ID = cp.sourceEntityId,
        SOURCE = cp.sourceName,
        SCM = cp.standardCommunicationChannel,
        STATE = cp.state,
        STREET = cp.street,
        TITLE = cp.title,
        ZIP_CODE = cp.zipCode,
        MIDDLE_NAME = Option.empty,
        ROLE = cp.jobTitle,
        ORG_FIRST_NAME = Option.empty,
        ORG_LAST_NAME = Option.empty,
        ORG_EMAIL_ADDRESS = Option.empty,
        ORG_FIXED_PHONE_NUMBER = Option.empty,
        ORG_MOBILE_PHONE_NUMBER = Option.empty,
        ORG_FAX_NUMBER = Option.empty,
        MOB_OPT_OUT_DATE = Option.empty
      )
    }
  }
}
