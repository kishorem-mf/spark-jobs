package com.unilever.ohub.spark.acm

import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.export.DeltaFunctions
import com.unilever.ohub.spark.acm.model.AcmOperator
import com.unilever.ohub.spark.data.ChannelMapping
import com.unilever.ohub.spark.domain.entity.Operator
import com.unilever.ohub.spark.sql.JoinType
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.DomainDataProvider
import org.apache.spark.sql.{ Dataset, SparkSession }
import scopt.OptionParser

object OperatorAcmConverter extends SparkJob[DefaultWithDeltaConfig]
  with DeltaFunctions with AcmTransformationFunctions with AcmConverter {

  def transform(
    spark: SparkSession,
    channelMappings: Dataset[ChannelMapping],
    operators: Dataset[Operator],
    previousIntegrated: Dataset[Operator]
  ): Dataset[AcmOperator] = {
    val dailyAcmOperators = createAcmOperators(spark, operators, channelMappings)
    val allPreviousAcmOperators = createAcmOperators(spark, previousIntegrated, channelMappings)

    integrate[AcmOperator](spark, dailyAcmOperators, allPreviousAcmOperators, "OPR_LNKD_INTEGRATION_ID")
  }

  override private[spark] def defaultConfig = DefaultWithDeltaConfig()

  override private[spark] def configParser(): OptionParser[DefaultWithDeltaConfig] = DefaultWithDeltaConfigParser()

  override def run(spark: SparkSession, config: DefaultWithDeltaConfig, storage: Storage): Unit = {
    import spark.implicits._

    log.info(s"Generating operator ACM csv file from [$config.inputFile] to [$config.outputFile]")

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
      options = extraWriteOptions
    )
  }

  def createAcmOperators(spark: SparkSession, operators: Dataset[Operator], channelMappings: Dataset[ChannelMapping]): Dataset[AcmOperator] = {
    import spark.implicits._
    val AcmOperatorRecords = operators
      .filter(_.isGoldenRecord)
      .map(operator ⇒
        operator match {
          case Operator(concatId, countryCode, customerType, dateCreated, dateUpdated, isActive, isGoldenRecord, ohubId, name, sourceEntityId, sourceName, ohubCreated, ohubUpdated, averagePrice, chainId, chainName, channel, city, cookingConvenienceLevel, countryName, daysOpen, distributorName, distributorOperatorId, emailAddress, faxNumber, hasDirectMailOptIn, hasDirectMailOptOut, hasEmailOptIn, hasEmailOptOut, hasFaxOptIn, hasFaxOptOut, hasGeneralOptOut, hasMobileOptIn, hasMobileOptOut, hasTelemarketingOptIn, hasTelemarketingOptOut, houseNumber, houseNumberExtension, isNotRecalculatingOtm, isOpenOnFriday, isOpenOnMonday, isOpenOnSaturday, isOpenOnSunday, isOpenOnThursday, isOpenOnTuesday, isOpenOnWednesday, isPrivateHousehold, kitchenType, mobileNumber, netPromoterScore, oldIntegrationId, otm, otmEnteredBy, phoneNumber, region, salesRepresentative, state, street, subChannel, totalDishes, totalLocations, totalStaff, vat, webUpdaterId, weeksClosed, zipCode, additionalFields, ingestionErrors) => AcmOperator(
            OPR_ORIG_INTEGRATION_ID = ohubId.getOrElse("UNKNOWN"),
            OPR_LNKD_INTEGRATION_ID = concatId,
            GOLDEN_RECORD_FLAG = boolAsString(isGoldenRecord),
            COUNTRY_CODE = countryCode,
            NAME = Option(name).map(clean),
            CHANNEL = channel,
            SUB_CHANNEL = subChannel,
            ROUTE_TO_MARKET = Option.empty,
            REGION = region,
            OTM = otm,
            PREFERRED_PARTNER = distributorName.map(clean),
            STREET = street,
            HOUSE_NUMBER = houseNumber,
            ZIPCODE = zipCode,
            CITY = city.map(clean),
            COUNTRY = countryName,
            AVERAGE_SELLING_PRICE = averagePrice.map(_.setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble),
            NUMBER_OF_COVERS = totalDishes,
            NUMBER_OF_WEEKS_OPEN = weeksClosed.map { weeksClosed ⇒
              if (52 - weeksClosed < 0) 0 else 52 - weeksClosed
            },
            NUMBER_OF_DAYS_OPEN = daysOpen,
            CONVENIENCE_LEVEL = cookingConvenienceLevel,
            RESPONSIBLE_EMPLOYEE = salesRepresentative,
            NPS_POTENTIAL = netPromoterScore,
            CAM_KEY = Option.empty,
            CAM_TEXT = Option.empty,
            CHANNEL_KEY = Option.empty,
            CHANNEL_TEXT = Option.empty,
            CHAIN_KNOTEN = chainId,
            CHAIN_NAME = chainName.map(clean),
            CUST_SUB_SEG_EXT = Option.empty,
            CUST_SEG_EXT = Option.empty,
            CUST_SEG_KEY_EXT = Option.empty,
            CUST_GRP_EXT = Option.empty,
            PARENT_SEGMENT = Option.empty,
            DATE_CREATED = dateCreated.map(formatWithPattern()),
            DATE_UPDATED = dateUpdated.map(formatWithPattern()),
            DELETE_FLAG = if (isActive) "N" else "Y",
            WHOLESALER_OPERATOR_ID = distributorOperatorId,
            PRIVATE_HOUSEHOLD = isPrivateHousehold.map(boolAsString),
            VAT = vat,
            OPEN_ON_MONDAY = isOpenOnMonday.map(boolAsString),
            OPEN_ON_TUESDAY = isOpenOnTuesday.map(boolAsString),
            OPEN_ON_WEDNESDAY = isOpenOnWednesday.map(boolAsString),
            OPEN_ON_THURSDAY = isOpenOnThursday.map(boolAsString),
            OPEN_ON_FRIDAY = isOpenOnFriday.map(boolAsString),
            OPEN_ON_SATURDAY = isOpenOnSaturday.map(boolAsString),
            OPEN_ON_SUNDAY = isOpenOnSunday.map(boolAsString),
            KITCHEN_TYPE = kitchenType.map(clean)
          )
        }
      )

    AcmOperatorRecords
      .joinWith(
        channelMappings,
        channelMappings("originalChannel") === AcmOperatorRecords("CHANNEL") and
          channelMappings("countryCode") === AcmOperatorRecords("COUNTRY_CODE"),
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
