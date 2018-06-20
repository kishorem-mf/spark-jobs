package com.unilever.ohub.spark.dispatcher

import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.Dispatcher.model.DispatcherContactPerson
import com.unilever.ohub.spark.domain.entity.ContactPerson
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.tsv2parquet.DomainDataProvider
import org.apache.spark.sql.{ Dataset, SparkSession }
import scopt.OptionParser

object ContactPersonDispatcherConverter extends SparkJob[DefaultWithDbAndDeltaConfig]
  with DeltaFunctions with DispatcherTransformationFunctions with DispatcherConverter {

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

  def createUfsContactPersons(
    spark: SparkSession,
    contactPersons: Dataset[ContactPerson]
  ): Dataset[DispatcherContactPerson] = {
    import spark.implicits._

    contactPersons.filter(_.isGoldenRecord).map { cp ⇒ // TODO check whether the filter is at the right location
      DispatcherContactPerson(
        DATE_OF_BIRTH = cp.birthDate.map(formatWithPattern()),
        CITY = cp.city,
        CP_ORIG_INTEGRATION_ID = cp.concatId,
        COUNTRY_CODE = cp.countryCode,
        COUNTRY = cp.countryName,
        EMAIL_ADDRESS = cp.emailAddress,
        CONFIRMED_OPT_IN_DATE = cp.,
        OPT_IN_DATE = cp.,
        FAX_NUMBER = cp.,
        GENDER = cp.,
        DM_OPT_OUT = cp.,
        CONFIRMED_OPT_IN = cp.,
        OPT_IN = cp.,
        EMAIL_OPT_OUT = cp.,
        FAX_OPT_OUT = cp.,
        MOB_CONFIRMED_OPT_IN = cp.,
        MOB_OPT_IN = cp.,
        MOBILE_OPT_OUT = cp.,
        FIXED_OPT_OUT = cp.,
        HOUSE_NUMBER = cp.,
        HOUSE_NUMBER_ADD = cp.,
        DELETE_FLAG = cp.,
        GOLDEN_RECORD_FLAG = cp.,
        KEY_DECISION_MAKER = cp.,
        PREFERRED = cp.,
        LANGUAGE = cp.,
        LAST_NAME = cp.,
        MOB_CONFIRMED_OPT_IN_DATE = cp.,
        MOBILE_PHONE_NUMBER = cp.,
        MOB_OPT_IN_DATE = cp.,
        CREATED_AT = cp.,
        CP_LNKD_INTEGRATION_ID = cp.,
        UPDATED_AT = cp.,
        OPR_ORIG_INTEGRATION_ID = cp.,
        FIXED_PHONE_NUMBER = cp.,
        SOURCE_ID = cp.,
        SOURCE = cp.,
        SCM = cp.,
        STATE = cp.,
        STREET = cp.,
        TITLE = cp.,
        ZIP_CODE = cp.,
        MIDDLE_NAME = cp.,
        ROLE = cp.,
        ORG_FIRST_NAME = cp.,
        ORG_LAST_NAME = cp.,
        ORG_EMAIL_ADDRESS = cp.,
        ORG_FIXED_PHONE_NUMBER = cp.,
        ORG_MOBILE_PHONE_NUMBER = cp.,
        ORG_FAX_NUMBER = cp.,
        MOB_OPT_OUT_DATE = cp.
      )
    }
  }

}
