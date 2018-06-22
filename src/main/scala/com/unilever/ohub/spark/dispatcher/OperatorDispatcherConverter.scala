package com.unilever.ohub.spark.dispatcher

import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.export.DeltaFunctions
import com.unilever.ohub.spark.dispatcher.model.DispatcherOperator
import com.unilever.ohub.spark.data.ChannelMapping
import com.unilever.ohub.spark.domain.entity.Operator
import com.unilever.ohub.spark.sql.JoinType
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.tsv2parquet.DomainDataProvider
import org.apache.spark.sql.{ Dataset, SparkSession }
import scopt.OptionParser

object OperatorDispatcherConverter extends SparkJob[DefaultWithDbAndDeltaConfig]
  with DeltaFunctions {

  def transform(
    spark: SparkSession,
    channelMappings: Dataset[ChannelMapping],
    operators: Dataset[Operator],
    previousIntegrated: Dataset[Operator]
  ): Dataset[DispatcherOperator] = {
    val dailyUfsOperators = createDispatcherOperators(spark, operators, channelMappings)
    val allPreviousUfsOperators = createDispatcherOperators(spark, previousIntegrated, channelMappings)

    integrate[DispatcherOperator](spark, dailyUfsOperators, allPreviousUfsOperators, "OPR_ORIG_INTEGRATION_ID")
  }

  override private[spark] def defaultConfig = DefaultWithDbAndDeltaConfig()

  override private[spark] def configParser(): OptionParser[DefaultWithDbAndDeltaConfig] = DefaultWithDbAndDeltaConfigParser()

  override def run(spark: SparkSession, config: DefaultWithDbAndDeltaConfig, storage: Storage): Unit = {
    import spark.implicits._

    log.info(s"Generating operator Dispatcher csv file from [$config.inputFile] to [$config.outputFile]")

    val dataProvider = DomainDataProvider(spark, config.postgressUrl, config.postgressDB, config.postgressUsername, config.postgressPassword)

    val channelMappings = dataProvider.channelMappings()
    val operators = storage.readFromParquet[Operator](config.inputFile)
    val previousIntegrated = config.previousIntegrated match {
      case Some(s) ⇒ storage.readFromParquet[Operator](s)
      case None ⇒
        log.warn(s"No existing integrated file specified -- regarding as initial load.")
        spark.emptyDataset[Operator]
    }
    val transformed = transform(spark, channelMappings, operators, previousIntegrated)

    storage.writeToSingleCsv(
      ds = transformed,
      outputFile = config.outputFile,
      options = EXTRA_WRITE_OPTIONS
    )
  }

  def createDispatcherOperators(
    spark: SparkSession,
    operators: Dataset[Operator],
    channelMappings: Dataset[ChannelMapping]
  ): Dataset[DispatcherOperator] = {
    import spark.implicits._

    val dispatcherOperators = operators.map { op ⇒
      DispatcherOperator(
        AVERAGE_SELLING_PRICE = op.averagePrice,
        CHAIN_KNOTEN = op.chainId,
        CHAIN_NAME = op.chainName,
        CHANNEL = op.channel,
        CITY = op.city,
        OPR_ORIG_INTEGRATION_ID = op.concatId,
        CONVENIENCE_LEVEL = op.cookingConvenienceLevel,
        COUNTRY_CODE = op.countryCode,
        COUNTRY = op.countryName,
        NUMBER_OF_DAYS_OPEN = op.daysOpen,
        WHOLE_SALER_OPERATOR_ID = op.distributorOperatorId,
        DM_OPT_OUT = op.hasDirectMailOptOut,
        EMAIL_OPT_OUT = op.hasEmailOptOut,
        FAX_OPT_OUT = op.hasFaxOptOut,
        MOBILE_OPT_OUT = op.hasMobileOptOut,
        FIXED_OPT_OUT = op.hasTelemarketingOptOut,
        HOUSE_NUMBER = op.houseNumber,
        HOUSE_NUMBER_EXT = op.houseNumberExtension,
        DELETE_FLAG = !op.isActive,
        GOLDEN_RECORD_FLAG = op.isGoldenRecord,
        OPEN_ON_FRIDAY = op.isOpenOnFriday,
        OPEN_ON_MONDAY = op.isOpenOnMonday,
        OPEN_ON_SATURDAY = op.isOpenOnSaturday,
        OPEN_ON_SUNDAY = op.isOpenOnSunday,
        OPEN_ON_THURSDAY = op.isOpenOnThursday,
        OPEN_ON_TUESDAY = op.isOpenOnTuesday,
        OPEN_ON_WEDNESDAY = op.isOpenOnWednesday,
        PRIVATE_HOUSEHOLD = op.isPrivateHousehold,
        KITCHEN_TYPE = op.kitchenType,
        NAME = op.name,
        NPS_POTENTIAL = op.netPromoterScore,
        CREATED_AT = formatWithPattern()(op.ohubCreated),
        OPR_LNKD_INTEGRATION_ID = op.ohubId,
        UPDATED_AT = formatWithPattern()(op.ohubUpdated),
        OTM = op.otm,
        REGION = op.region,
        SOURCE_ID = op.sourceEntityId,
        SOURCE = op.sourceName,
        STATE = op.state,
        STREET = op.street,
        SUB_CHANNEL = op.subChannel,
        NUMBER_OF_COVERS = op.totalDishes,
        VAT = op.vat,
        NUMBER_OF_WEEKS_OPEN = op.weeksClosed.map((closed: Int) ⇒ 52 - closed),
        ZIP_CODE = op.zipCode,
        ROUTE_TO_MARKET = None,
        PREFERRED_PARTNER = "-2",
        STATUS = None, // "status of the operator, for example: seasonal, D, A, closed
        RESPONSIBLE_EMPLOYEE = None,
        CAM_KEY = None,
        CAM_TEXT = None,
        CHANNEL_KEY = None,
        CHANNEL_TEXT = None,
        LOCAL_CHANNEL = None, // set below
        CHANNEL_USAGE = None, // set below
        SOCIAL_COMMERCIAL = None, // set below
        STRATEGIC_CHANNEL = None, // set below
        GLOBAL_CHANNEL = None, // set below
        GLOBAL_SUBCHANNEL = None // set below
      )
    }

    dispatcherOperators
      .joinWith(
        channelMappings,
        channelMappings("originalChannel") === dispatcherOperators("CHANNEL") and
          channelMappings("countryCode") === dispatcherOperators("COUNTRY_CODE"),
        JoinType.Left
      )
      .map {
        case (operator, maybeChannelMapping) ⇒ Option(maybeChannelMapping).fold(operator) { channelMapping ⇒
          operator.copy(
            LOCAL_CHANNEL = Option(channelMapping.localChannel),
            CHANNEL_USAGE = Option(channelMapping.channelUsage),
            SOCIAL_COMMERCIAL = Option(channelMapping.socialCommercial),
            STRATEGIC_CHANNEL = Option(channelMapping.strategicChannel),
            GLOBAL_CHANNEL = Option(channelMapping.globalChannel),
            GLOBAL_SUBCHANNEL = Option(channelMapping.globalSubChannel)
          )
        }
      }
  }

}
