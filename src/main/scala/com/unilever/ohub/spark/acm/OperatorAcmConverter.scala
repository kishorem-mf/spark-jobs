package com.unilever.ohub.spark.acm

import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.data.{ ChannelMapping, GoldenOperatorRecord }
import com.unilever.ohub.spark.data.ufs.UFSOperator
import com.unilever.ohub.spark.generic.StringFunctions
import com.unilever.ohub.spark.sql.JoinType
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{ Dataset, SparkSession }

object OperatorAcmConverter extends SparkJob {
  private val boolAsString = (bool: Boolean) => if (bool) "Y" else "N"
  private val clean = (str: String) => StringFunctions.removeGenericStrangeChars(str)

  def transform(
    spark: SparkSession,
    channelMappings: Dataset[ChannelMapping],
    oHubIdAndOperators: Dataset[GoldenOperatorRecord]
  ): Dataset[UFSOperator] = {
    import spark.implicits._

    val ufsOperators = oHubIdAndOperators.map { goldenOperator =>
      val operator = goldenOperator.operator

      UFSOperator(
        OPR_ORIG_INTEGRATION_ID = goldenOperator.ohubOperatorId,
        OPR_LNKD_INTEGRATION_ID = operator.operatorConcatId,
        GOLDEN_RECORD_FLAG = "Y",
        COUNTRY_CODE = operator.countryCode,
        NAME = operator.name.map(clean),
        CHANNEL = operator.channel,
        SUB_CHANNEL = operator.subChannel,
        ROUTE_TO_MARKET = "",
        REGION = operator.region,
        OTM = operator.otm,
        PREFERRED_PARTNER = operator.distributorName.map(clean),
        STREET = operator.street,
        HOUSE_NUMBER = operator.housenumber,
        ZIPCODE = operator.zipCode,
        CITY = operator.city.map(clean),
        COUNTRY = operator.country,
        AVERAGE_SELLING_PRICE = operator.avgPrice.map(_.setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble),
        NUMBER_OF_COVERS = operator.nrOfDishes,
        NUMBER_OF_WEEKS_OPEN = operator.weeksClosed.map { weeksClosed =>
          if (52 - weeksClosed < 0) 0 else 52 - weeksClosed
        },
        NUMBER_OF_DAYS_OPEN = operator.daysOpen,
        CONVENIENCE_LEVEL = operator.convenienceLevel,
        RESPONSIBLE_EMPLOYEE = operator.salesRep,
        NPS_POTENTIAL = operator.npsPotential,
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
        DATE_CREATED = operator.dateCreated,
        DATE_UPDATED = operator.dateModified,
        DELETE_FLAG = operator.status.map(status => if (status) "N" else "Y"),
        WHOLESALER_OPERATOR_ID = operator.distributorCustomerNr,
        PRIVATE_HOUSEHOLD = operator.privateHousehold.map(boolAsString),
        VAT = operator.vatNumber,
        OPEN_ON_MONDAY = operator.openOnMonday.map(boolAsString),
        OPEN_ON_TUESDAY = operator.openOnTuesday.map(boolAsString),
        OPEN_ON_WEDNESDAY = operator.openOnWednesday.map(boolAsString),
        OPEN_ON_THURSDAY = operator.openOnThursday.map(boolAsString),
        OPEN_ON_FRIDAY = operator.openOnFriday.map(boolAsString),
        OPEN_ON_SATURDAY = operator.openOnSaturday.map(boolAsString),
        OPEN_ON_SUNDAY = operator.openOnSunday.map(boolAsString),
        KITCHEN_TYPE = operator.kitchenType.map(clean)
      )
    }

    ufsOperators
      .joinWith(
        channelMappings,
        channelMappings("ORIGINAL_CHANNEL") === ufsOperators("CHANNEL") and
          channelMappings("COUNTRY_CODE") === ufsOperators("COUNTRY_CODE"),
        JoinType.Left
      )
      .map {
        case (operator, channelMapping) => operator.copy(
          LOCAL_CHANNEL = Option(channelMapping.LOCAL_CHANNEL),
          CHANNEL_USAGE = Option(channelMapping.CHANNEL_USAGE),
          SOCIAL_COMMERCIAL = Option(channelMapping.SOCIAL_COMMERCIAL),
          STRATEGIC_CHANNEL = Option(channelMapping.STRATEGIC_CHANNEL),
          GLOBAL_CHANNEL = Option(channelMapping.GLOBAL_CHANNEL),
          GLOBAL_SUBCHANNEL = Option(channelMapping.GLOBAL_SUBCHANNEL)
        )
      }
  }

  override val neededFilePaths = Array("INPUT_FILE", "OUTPUT_FILE")

  override def run(spark: SparkSession, filePaths: scala.Product, storage: Storage): Unit = {
    import spark.implicits._

    val (inputFile: String, outputFile: String) = filePaths

    log.info(s"Generating operator ACM csv file from [$inputFile] to [$outputFile]")

    val channelMappings = storage.channelMappings

    val operators = storage
      .readFromParquet[GoldenOperatorRecord](inputFile)

    val transformed = transform(spark, channelMappings, operators)

    storage
      .writeToCSV(transformed, outputFile, partitionBy = "COUNTRY_CODE")
  }
}
