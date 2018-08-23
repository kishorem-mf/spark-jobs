package com.unilever.ohub.spark.dispatcher

import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.export.DeltaFunctions
import com.unilever.ohub.spark.dispatcher.model.DispatcherOperator
import com.unilever.ohub.spark.data.ChannelMapping
import com.unilever.ohub.spark.domain.entity.Operator
import com.unilever.ohub.spark.sql.JoinType
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.DomainDataProvider
import org.apache.spark.sql.{ Dataset, SparkSession }
import scopt.OptionParser

object OperatorDispatcherConverter extends SparkJob[DefaultWithDeltaConfig]
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

  override private[spark] def defaultConfig = DefaultWithDeltaConfig()

  override private[spark] def configParser(): OptionParser[DefaultWithDeltaConfig] = DefaultWithDeltaConfigParser()

  override def run(spark: SparkSession, config: DefaultWithDeltaConfig, storage: Storage): Unit = {
    import spark.implicits._

    log.info(s"Generating operator Dispatcher csv file from [$config.inputFile] to [$config.outputFile]")

    val dataProvider = DomainDataProvider(spark)

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
      op match {
        case Operator(concatId, countryCode, customerType, dateCreated, dateUpdated, isActive, isGoldenRecord, ohubId, name, sourceEntityId, sourceName, ohubCreated, ohubUpdated, averagePrice, chainId, chainName, channel, city, cookingConvenienceLevel, countryName, daysOpen, distributorName, distributorOperatorId, emailAddress, faxNumber, hasDirectMailOptIn, hasDirectMailOptOut, hasEmailOptIn, hasEmailOptOut, hasFaxOptIn, hasFaxOptOut, hasGeneralOptOut, hasMobileOptIn, hasMobileOptOut, hasTelemarketingOptIn, hasTelemarketingOptOut, houseNumber, houseNumberExtension, isNotRecalculatingOtm, isOpenOnFriday, isOpenOnMonday, isOpenOnSaturday, isOpenOnSunday, isOpenOnThursday, isOpenOnTuesday, isOpenOnWednesday, isPrivateHousehold, kitchenType, mobileNumber, netPromoterScore, oldIntegrationId, otm, otmEnteredBy, phoneNumber, region, salesRepresentative, state, street, subChannel, totalDishes, totalLocations, totalStaff, vat, webUpdaterId, weeksClosed, zipCode, additionalFields, ingestionErrors) ⇒ DispatcherOperator(
          OPR_ORIG_INTEGRATION_ID = concatId,
          OPR_LNKD_INTEGRATION_ID = ohubId,
          AVERAGE_SELLING_PRICE = averagePrice,
          CHAIN_KNOTEN = chainId,
          CHAIN_NAME = chainName,
          CHANNEL = channel,
          CITY = city,
          CONVENIENCE_LEVEL = cookingConvenienceLevel,
          COUNTRY_CODE = countryCode,
          COUNTRY = countryName,
          NUMBER_OF_DAYS_OPEN = daysOpen,
          WHOLE_SALER_OPERATOR_ID = distributorOperatorId,
          DM_OPT_OUT = hasDirectMailOptOut,
          EMAIL_OPT_OUT = hasEmailOptOut,
          FAX_OPT_OUT = hasFaxOptOut,
          MOBILE_OPT_OUT = hasMobileOptOut,
          FIXED_OPT_OUT = hasTelemarketingOptOut,
          HOUSE_NUMBER = houseNumber,
          HOUSE_NUMBER_EXT = houseNumberExtension,
          DELETE_FLAG = !isActive,
          GOLDEN_RECORD_FLAG = isGoldenRecord,
          OPEN_ON_FRIDAY = isOpenOnFriday,
          OPEN_ON_MONDAY = isOpenOnMonday,
          OPEN_ON_SATURDAY = isOpenOnSaturday,
          OPEN_ON_SUNDAY = isOpenOnSunday,
          OPEN_ON_THURSDAY = isOpenOnThursday,
          OPEN_ON_TUESDAY = isOpenOnTuesday,
          OPEN_ON_WEDNESDAY = isOpenOnWednesday,
          PRIVATE_HOUSEHOLD = isPrivateHousehold,
          KITCHEN_TYPE = kitchenType,
          NAME = name,
          NPS_POTENTIAL = netPromoterScore,
          CREATED_AT = formatWithPattern()(ohubCreated),
          UPDATED_AT = formatWithPattern()(ohubUpdated),
          OTM = otm,
          REGION = region,
          SOURCE_ID = sourceEntityId,
          SOURCE = sourceName,
          STATE = state,
          STREET = street,
          SUB_CHANNEL = subChannel,
          NUMBER_OF_COVERS = totalDishes,
          VAT = vat,
          NUMBER_OF_WEEKS_OPEN = weeksClosed.map { weeksClosed ⇒
            if (52 - weeksClosed < 0) 0 else 52 - weeksClosed
          },
          ZIP_CODE = zipCode,
          ROUTE_TO_MARKET = None,
          PREFERRED_PARTNER = Some("-2"),
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
