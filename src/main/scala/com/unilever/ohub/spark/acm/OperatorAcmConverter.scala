package com.unilever.ohub.spark.acm

import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.acm.model.AcmOperator
import com.unilever.ohub.spark.data.ChannelMapping
import com.unilever.ohub.spark.domain.entity.Operator
import com.unilever.ohub.spark.sql.JoinType
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.tsv2parquet.DomainDataProvider
import org.apache.spark.sql.{ Dataset, SparkSession }
import scopt.OptionParser

object OperatorAcmConverter extends SparkJob[DefaultWithDbAndDeltaConfig]
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

  override private[spark] def defaultConfig = DefaultWithDbAndDeltaConfig()

  override private[spark] def configParser(): OptionParser[DefaultWithDbAndDeltaConfig] = DefaultWithDbAndDeltaConfigParser()

  override def run(spark: SparkSession, config: DefaultWithDbAndDeltaConfig, storage: Storage): Unit = {
    import spark.implicits._

    log.info(s"Generating operator ACM csv file from [$config.inputFile] to [$config.outputFile]")

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
      options = extraWriteOptions
    )
  }

  def createAcmOperators(spark: SparkSession, operators: Dataset[Operator], channelMappings: Dataset[ChannelMapping]): Dataset[AcmOperator] = {
    import spark.implicits._
    val AcmOperatorRecords = operators
      .filter(_.isGoldenRecord)
      .map(operator ⇒
        AcmOperator(
          OPR_ORIG_INTEGRATION_ID = operator.ohubId.getOrElse("UNKNOWN"),
          OPR_LNKD_INTEGRATION_ID = operator.concatId,
          GOLDEN_RECORD_FLAG = boolAsString(operator.isGoldenRecord),
          COUNTRY_CODE = operator.countryCode,
          NAME = clean(operator.name),
          CHANNEL = operator.channel,
          SUB_CHANNEL = operator.subChannel,
          ROUTE_TO_MARKET = "",
          REGION = operator.region,
          OTM = operator.otm,
          PREFERRED_PARTNER = operator.distributorName.map(clean),
          STREET = operator.street,
          HOUSE_NUMBER = operator.houseNumber,
          ZIPCODE = operator.zipCode,
          CITY = operator.city.map(clean),
          COUNTRY = operator.countryName,
          AVERAGE_SELLING_PRICE = operator.averagePrice.map(_.setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble),
          NUMBER_OF_COVERS = operator.totalDishes,
          NUMBER_OF_WEEKS_OPEN = operator.weeksClosed.map { weeksClosed ⇒
            if (52 - weeksClosed < 0) 0 else 52 - weeksClosed
          },
          NUMBER_OF_DAYS_OPEN = operator.daysOpen,
          CONVENIENCE_LEVEL = operator.cookingConvenienceLevel,
          RESPONSIBLE_EMPLOYEE = operator.salesRepresentative,
          NPS_POTENTIAL = operator.netPromoterScore,
          CAM_KEY = "",
          CAM_TEXT = "",
          CHANNEL_KEY = "",
          CHANNEL_TEXT = "",
          CHAIN_KNOTEN = operator.chainId,
          CHAIN_NAME = operator.chainName.map(clean),
          CUST_SUB_SEG_EXT = "",
          CUST_SEG_EXT = "",
          CUST_SEG_KEY_EXT = "",
          CUST_GRP_EXT = "",
          PARENT_SEGMENT = "",
          DATE_CREATED = operator.dateCreated.map(formatWithPattern()),
          DATE_UPDATED = operator.dateUpdated.map(formatWithPattern()),
          DELETE_FLAG = if (operator.isActive) "N" else "Y",
          WHOLESALER_OPERATOR_ID = operator.distributorOperatorId,
          PRIVATE_HOUSEHOLD = operator.isPrivateHousehold.map(boolAsString),
          VAT = operator.vat,
          OPEN_ON_MONDAY = operator.isOpenOnMonday.map(boolAsString),
          OPEN_ON_TUESDAY = operator.isOpenOnTuesday.map(boolAsString),
          OPEN_ON_WEDNESDAY = operator.isOpenOnWednesday.map(boolAsString),
          OPEN_ON_THURSDAY = operator.isOpenOnThursday.map(boolAsString),
          OPEN_ON_FRIDAY = operator.isOpenOnFriday.map(boolAsString),
          OPEN_ON_SATURDAY = operator.isOpenOnSaturday.map(boolAsString),
          OPEN_ON_SUNDAY = operator.isOpenOnSunday.map(boolAsString),
          KITCHEN_TYPE = operator.kitchenType.map(clean)
        )
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
