package com.unilever.ohub.spark.dispatcher

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.data.ChannelMapping
import com.unilever.ohub.spark.dispatcher.model.DispatcherOperator
import com.unilever.ohub.spark.domain.entity.{ Operator, TestOperators }
import org.apache.spark.sql.Dataset
import cats.syntax.option._

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
            .map(_.copy(ohubId = "randomId"))
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
        OPR_LNKD_INTEGRATION_ID = "randomId",
        AVERAGE_SELLING_PRICE = BigDecimal(12345),
        CHAIN_KNOTEN = "chain-id",
        CHAIN_NAME = "chain-name",
        CHANNEL = "channel",
        CITY = "city",
        CONVENIENCE_LEVEL = "cooking-convenience-level",
        COUNTRY_CODE = "country-code",
        COUNTRY = "country-name",
        NUMBER_OF_DAYS_OPEN = Some(4),
        WHOLE_SALER_OPERATOR_ID = none,
        DM_OPT_OUT = false,
        EMAIL_OPT_OUT = false,
        FAX_OPT_OUT = false,
        MOBILE_OPT_OUT = false,
        FIXED_OPT_OUT = false,
        HOUSE_NUMBER = "12",
        HOUSE_NUMBER_EXT = none,
        DELETE_FLAG = false,
        GOLDEN_RECORD_FLAG = true,
        OPEN_ON_FRIDAY = true,
        OPEN_ON_MONDAY = false,
        OPEN_ON_SATURDAY = true,
        OPEN_ON_SUNDAY = false,
        OPEN_ON_THURSDAY = true,
        OPEN_ON_TUESDAY = true,
        OPEN_ON_WEDNESDAY = true,
        PRIVATE_HOUSEHOLD = false,
        KITCHEN_TYPE = "kitchen-type",
        NAME = "operator-name",
        NPS_POTENTIAL = BigDecimal(75),
        CREATED_AT = "2017-11-16 18:09:49",
        UPDATED_AT = "2017-11-16 18:09:49",
        OTM = "D",
        REGION = "region",
        SOURCE_ID = "source-entity-id",
        SOURCE = "source-name",
        STATE = "state",
        STREET = "street",
        SUB_CHANNEL = "sub-channel",
        NUMBER_OF_COVERS = Some(150),
        VAT = "vat",
        NUMBER_OF_WEEKS_OPEN = Some(50),
        ZIP_CODE = "1234 AB",
        ROUTE_TO_MARKET = none,
        PREFERRED_PARTNER = "-2",
        STATUS = none,
        RESPONSIBLE_EMPLOYEE = none,
        CAM_KEY = none,
        CAM_TEXT = none,
        CHANNEL_KEY = none,
        CHANNEL_TEXT = none,
        LOCAL_CHANNEL = "local-channel",
        CHANNEL_USAGE = "channel-usage",
        SOCIAL_COMMERCIAL = "social-commercial",
        STRATEGIC_CHANNEL = "strategic-channel",
        GLOBAL_CHANNEL = "global-channel",
        GLOBAL_SUBCHANNEL = "global-sub-channel"
      ))
    }
  }
}
