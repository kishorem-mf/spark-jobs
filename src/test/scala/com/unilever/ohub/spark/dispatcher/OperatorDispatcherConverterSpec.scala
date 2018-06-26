package com.unilever.ohub.spark.dispatcher

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.data.ChannelMapping
import com.unilever.ohub.spark.dispatcher.model.DispatcherOperator
import com.unilever.ohub.spark.domain.entity.{ Operator, TestOperators }
import org.apache.spark.sql.Dataset

class OperatorDispatcherConverterSpec extends SparkJobSpec with TestOperators {

  private val OperatorConverter = OperatorDispatcherConverter

  describe("contact person dispatcher delta converter") {
    it("should convert a domain contact person correctly into an dispatcher converter containing only delta records") {
      import spark.implicits._

      val channelMapping = ChannelMapping(countryCode = "country-code", originalChannel = "channel", localChannel = "local-channel", channelUsage = "channel-usage", socialCommercial = "social-commercial", strategicChannel = "strategic-channel", globalChannel = "global-channel", globalSubChannel = "global-sub-channel")
      val channelMappings: Dataset[ChannelMapping] = spark.createDataset(Seq(channelMapping))

      /**
       * Input file containing Operator records
       */
      val input: Dataset[Operator] = {
        spark.createDataset(
          List(defaultOperator)
            .map(_.copy(isGoldenRecord = true))
            .map(_.copy(ohubId = Some("randomId")))
        )
      }

      val emptyDataset: Dataset[Operator] = spark.emptyDataset[Operator]

      /**
       * Transformed DispatcherOperator
       */
      val result: List[DispatcherOperator] = {
        OperatorConverter.transform(spark, channelMappings, input, emptyDataset).collect().toList
      }

      result should contain(DispatcherOperator(
        OPR_ORIG_INTEGRATION_ID = "country-code~source-name~source-entity-id",
        OPR_LNKD_INTEGRATION_ID = Some("randomId"),
        AVERAGE_SELLING_PRICE = Some(BigDecimal(12345)),
        CHAIN_KNOTEN = Some("chain-id"),
        CHAIN_NAME = Some("chain-name"),
        CHANNEL = Some("channel"),
        CITY = Some("city"),
        CONVENIENCE_LEVEL = Some("cooking-convenience-level"),
        COUNTRY_CODE = "country-code",
        COUNTRY = Some("country-name"),
        NUMBER_OF_DAYS_OPEN = Some(4),
        WHOLE_SALER_OPERATOR_ID = None,
        DM_OPT_OUT = Some(false),
        EMAIL_OPT_OUT = Some(false),
        FAX_OPT_OUT = Some(false),
        MOBILE_OPT_OUT = Some(false),
        FIXED_OPT_OUT = Some(false),
        HOUSE_NUMBER = Some("12"),
        HOUSE_NUMBER_EXT = None,
        DELETE_FLAG = false,
        GOLDEN_RECORD_FLAG = true,
        OPEN_ON_FRIDAY = Some(true),
        OPEN_ON_MONDAY = Some(false),
        OPEN_ON_SATURDAY = Some(true),
        OPEN_ON_SUNDAY = Some(false),
        OPEN_ON_THURSDAY = Some(true),
        OPEN_ON_TUESDAY = Some(true),
        OPEN_ON_WEDNESDAY = Some(true),
        PRIVATE_HOUSEHOLD = Some(false),
        KITCHEN_TYPE = Some("kitchen-type"),
        NAME = "operator-name",
        NPS_POTENTIAL = Some(BigDecimal(75)),
        CREATED_AT = "2017-11-16 18:09:49",
        UPDATED_AT = "2017-11-16 18:09:49",
        OTM = Some("D"),
        REGION = Some("region"),
        SOURCE_ID = "source-entity-id",
        SOURCE = "source-name",
        STATE = Some("state"),
        STREET = Some("street"),
        SUB_CHANNEL = Some("sub-channel"),
        NUMBER_OF_COVERS = Some(150),
        VAT = Some("vat"),
        NUMBER_OF_WEEKS_OPEN = Some(50),
        ZIP_CODE = Some("1234 AB"),
        ROUTE_TO_MARKET = None,
        PREFERRED_PARTNER = Some("-2"),
        STATUS = None,
        RESPONSIBLE_EMPLOYEE = None,
        CAM_KEY = None,
        CAM_TEXT = None,
        CHANNEL_KEY = None,
        CHANNEL_TEXT = None,
        LOCAL_CHANNEL = Some("local-channel"),
        CHANNEL_USAGE = Some("channel-usage"),
        SOCIAL_COMMERCIAL = Some("social-commercial"),
        STRATEGIC_CHANNEL = Some("strategic-channel"),
        GLOBAL_CHANNEL = Some("global-channel"),
        GLOBAL_SUBCHANNEL = Some("global-sub-channel")
      ))
    }
  }
}
