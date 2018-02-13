package com.unilever.ohub.spark.acm

import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.generic.{ SparkFunctions, StringFunctions }
import com.unilever.ohub.spark.sql.JoinType
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{ Dataset, SparkSession }

case class ChannelMapping(
  LOCAL_CHANNEL: String,
  CHANNEL_USAGE: String,
  SOCIAL_COMMERCIAL: String,
  STRATEGIC_CHANNEL: String,
  GLOBAL_CHANNEL: String,
  GLOBAL_SUBCHANNEL: String
)

case class Operator(
  OHUB_OPERATOR_ID: String,
  OPERATOR_CONCAT_ID: String,
  COUNTRY_CODE: String,
  NAME: String,
  CHANNEL: String,
  SUB_CHANNEL: String,
  REGION: String,
  OTM: String,
  DISTRIBUTOR_NAME: String,
  STREET: String,
  HOUSENUMBER: String,
  ZIP_CODE: String,
  CITY: String,
  COUNTRY: String,
  AVG_PRICE: Double,
  NR_OF_DISHES: Int,
  WEEKS_CLOSED: Int,
  DAYS_OPEN: Int,
  CONVENIENCE_LEVEL: String,
  SALES_REP: String,
  NPS_POTENTIAL: String,
  CHAIN_ID: String,
  CHAIN_NAME: String,
  DATE_CREATED: String,
  DATE_MODIFIED: String,
  STATUS: Boolean,
  DISTRIBUTOR_CUSTOMER_NR: String,
  PRIVATE_HOUSEHOLD: Boolean,
  VAT_NUMBER: String,
  OPEN_ON_MONDAY: Boolean,
  OPEN_ON_TUESDAY: Boolean,
  OPEN_ON_WEDNESDAY: Boolean,
  OPEN_ON_THURSDAY: Boolean,
  OPEN_ON_FRIDAY: Boolean,
  OPEN_ON_SATURDAY: Boolean,
  OPEN_ON_SUNDAY: Boolean,
  KITCHEN_TYPE: String
)

case class OHubOperatorIdAndOperators(OHUB_OPERATOR_ID: String, OPERATOR: Seq[Operator])

// Data Model: OPR_ORIG_INTEGRATION_ID can be misleading for Ohub 2.0 as this will contain the new
// OHUB_OPERATOR_ID and OPR_LNKD_INTEGRATION_ID will contain OPERATOR_CONCAT_ID
case class UfsOperator(
  OPR_ORIG_INTEGRATION_ID: String,
  OPR_LNKD_INTEGRATION_ID: String,
  GOLDEN_RECORD_FLAG: String,
  COUNTRY_CODE: String,
  NAME: String,
  CHANNEL: String,
  SUB_CHANNEL: String,
  ROUTE_TO_MARKET: String,
  REGION: String,
  OTM: String,
  PREFERRED_PARTNER: String,
  STREET: String,
  HOUSE_NUMBER: String,
  ZIPCODE: String,
  CITY: String,
  COUNTRY: String,
  AVERAGE_SELLING_PRICE: Double,
  NUMBER_OF_COVERS: Int,
  NUMBER_OF_WEEKS_OPEN: Int,
  NUMBER_OF_DAYS_OPEN: Int,
  CONVENIENCE_LEVEL: String,
  RESPONSIBLE_EMPLOYEE: String,
  NPS_POTENTIAL: String,
  CAM_KEY: String,
  CAM_TEXT: String,
  CHANNEL_KEY: String,
  CHANNEL_TEXT: String,
  CHAIN_KNOTEN: String,
  CHAIN_NAME: String,
  CUST_SUB_SEG_EXT: String,
  CUST_SEG_EXT: String,
  CUST_SEG_KEY_EXT: String,
  CUST_GRP_EXT: String,
  PARENT_SEGMENT: String,
  DATE_CREATED: String,
  DATE_UPDATED: String,
  DELETE_FLAG: String,
  WHOLESALER_OPERATOR_ID: String,
  PRIVATE_HOUSEHOLD: String,
  VAT: String,
  OPEN_ON_MONDAY: String,
  OPEN_ON_TUESDAY: String,
  OPEN_ON_WEDNESDAY: String,
  OPEN_ON_THURSDAY: String,
  OPEN_ON_FRIDAY: String,
  OPEN_ON_SATURDAY: String,
  OPEN_ON_SUNDAY: String,
  KITCHEN_TYPE: String,
  LOCAL_CHANNEL: Option[String] = None,
  CHANNEL_USAGE: Option[String] = None,
  SOCIAL_COMMERCIAL: Option[String] = None,
  STRATEGIC_CHANNEL: Option[String] = None,
  GLOBAL_CHANNEL: Option[String] = None,
  GLOBAL_SUBCHANNEL: Option[String] = None
)

object OperatorAcmConverter extends SparkJob {
  def transform(
    spark: SparkSession,
    channelMappings: Dataset[ChannelMapping],
    oHubIdAndOperators: Dataset[OHubOperatorIdAndOperators]
  ): Dataset[UfsOperator] = {
    import spark.implicits._

    val boolAsString = (bool: Boolean) => if (bool) "Y" else "N"
    val clean = (str: String) => StringFunctions.removeGenericStrangeChars(str)

    val operators = oHubIdAndOperators.flatMap(_.OPERATOR.map(operator => UfsOperator(
      OPR_ORIG_INTEGRATION_ID = operator.OHUB_OPERATOR_ID,
      OPR_LNKD_INTEGRATION_ID = operator.OPERATOR_CONCAT_ID,
      GOLDEN_RECORD_FLAG = "Y",
      COUNTRY_CODE = operator.COUNTRY_CODE,
      NAME = clean(operator.NAME),
      CHANNEL = operator.CHANNEL,
      SUB_CHANNEL = operator.SUB_CHANNEL,
      ROUTE_TO_MARKET = "",
      REGION = operator.REGION,
      OTM = operator.OTM,
      PREFERRED_PARTNER = clean(operator.DISTRIBUTOR_NAME),
      STREET = operator.STREET,
      HOUSE_NUMBER = operator.HOUSENUMBER,
      ZIPCODE = operator.ZIP_CODE,
      CITY = clean(operator.CITY),
      COUNTRY = operator.COUNTRY,
      AVERAGE_SELLING_PRICE = BigDecimal(operator.AVG_PRICE)
        .setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble,
      NUMBER_OF_COVERS = operator.NR_OF_DISHES,
      NUMBER_OF_WEEKS_OPEN = if (52 - operator.WEEKS_CLOSED < 0) 0 else 52 - operator.WEEKS_CLOSED,
      NUMBER_OF_DAYS_OPEN = operator.DAYS_OPEN,
      CONVENIENCE_LEVEL = operator.CONVENIENCE_LEVEL,
      RESPONSIBLE_EMPLOYEE = operator.SALES_REP,
      NPS_POTENTIAL = operator.NPS_POTENTIAL,
      CAM_KEY = "",
      CAM_TEXT = "",
      CHANNEL_KEY = "",
      CHANNEL_TEXT = "",
      CHAIN_KNOTEN = operator.CHAIN_ID,
      CHAIN_NAME = clean(operator.CHAIN_NAME),
      CUST_SUB_SEG_EXT = "",
      CUST_SEG_EXT = "",
      CUST_SEG_KEY_EXT = "",
      CUST_GRP_EXT = "",
      PARENT_SEGMENT = "",
      DATE_CREATED = operator.DATE_CREATED,
      DATE_UPDATED = operator.DATE_MODIFIED,
      DELETE_FLAG = if (operator.STATUS) "N" else "Y",
      WHOLESALER_OPERATOR_ID = operator.DISTRIBUTOR_CUSTOMER_NR,
      PRIVATE_HOUSEHOLD = boolAsString(operator.PRIVATE_HOUSEHOLD),
      VAT = operator.VAT_NUMBER,
      OPEN_ON_MONDAY = boolAsString(operator.OPEN_ON_MONDAY),
      OPEN_ON_TUESDAY = boolAsString(operator.OPEN_ON_TUESDAY),
      OPEN_ON_WEDNESDAY = boolAsString(operator.OPEN_ON_WEDNESDAY),
      OPEN_ON_THURSDAY = boolAsString(operator.OPEN_ON_THURSDAY),
      OPEN_ON_FRIDAY = boolAsString(operator.OPEN_ON_FRIDAY),
      OPEN_ON_SATURDAY = boolAsString(operator.OPEN_ON_SATURDAY),
      OPEN_ON_SUNDAY = boolAsString(operator.OPEN_ON_SUNDAY),
      KITCHEN_TYPE = clean(operator.KITCHEN_TYPE)
    )))

    operators
      .joinWith(
        channelMappings,
        channelMappings("ORIGINAL_CHANNEL") === operators("CHANNEL") and
          channelMappings("COUNTRY_CODE") === operators("COUNTRY_CODE"),
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

    val channelMappingDF = SparkFunctions.readJdbcTable(spark, dbTable = "channel_mapping")
    val channelReferencesDF = SparkFunctions.readJdbcTable(spark, dbTable = "channel_references")
    val channelMappings = channelMappingDF
      .join(
        channelReferencesDF,
        col("channel_reference_fk") === col("channel_reference_id"),
        JoinType.Left
      )
      .select(
        $"LOCAL_CHANNEL",
        $"CHANNEL_USAGE",
        $"SOCIAL_COMMERCIAL",
        $"STRATEGIC_CHANNEL",
        $"GLOBAL_CHANNEL",
        $"GLOBAL_SUBCHANNEL"
      )
      .as[ChannelMapping]

    val operators = storage
      .readFromParquet[OHubOperatorIdAndOperators](
      inputFile,
      selectColumns = $"OHUB_OPERATOR_ID", $"OPERATOR.*"
    )

    val transformed = transform(spark, channelMappings, operators)

    storage
      .writeToCSV(transformed, outputFile, partitionBy = "COUNTRY_CODE")
  }
}
