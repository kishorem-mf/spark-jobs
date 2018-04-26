package com.unilever.ohub.spark.acm

import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.acm.model.UFSOperator
import com.unilever.ohub.spark.data.ChannelMapping
import com.unilever.ohub.spark.domain.entity.{ Operator, TestOperators }
import org.apache.spark.sql.Dataset

class OperatorAcmInitialLoadConverterSpec extends SparkJobSpec with TestOperators {

  private[acm] val SUT = OperatorAcmInitialLoadConverter

  describe("operator acm converter") {
    it("should convert a domain operator correctly into an acm converter") {
      import spark.implicits._

      val channelMapping = ChannelMapping(countryCode = "country-code", originalChannel = "channel", localChannel = "local-channel", channelUsage = "channel-usage", socialCommercial = "social-commercial", strategicChannel = "strategic-channel", globalChannel = "global-channel", globalSubChannel = "global-sub-channel")
      val channelMappings: Dataset[ChannelMapping] = spark.createDataset(Seq(channelMapping))
      val input: Dataset[Operator] = spark.createDataset(Seq(defaultOperator.copy(isGoldenRecord = true)))
      val result = SUT.transform(spark, channelMappings, input)

      result.count() shouldBe 1

      val actualAcmOperator = result.head()
      val expectedAcmOperator =
        UFSOperator(
          OPR_ORIG_INTEGRATION_ID = defaultOperator.ohubId.get,
          OPR_LNKD_INTEGRATION_ID = "country-code~source-name~source-entity-id",
          GOLDEN_RECORD_FLAG = "Y",
          COUNTRY_CODE = "country-code",
          NAME = "operatorname",
          CHANNEL = Some("channel"),
          SUB_CHANNEL = Some("sub-channel"),
          ROUTE_TO_MARKET = "",
          REGION = Some("region"),
          OTM = Some("D"),
          PREFERRED_PARTNER = Some("distributorname"),
          STREET = Some("street"),
          HOUSE_NUMBER = Some("12"),
          ZIPCODE = Some("1234 AB"),
          CITY = Some("city"),
          COUNTRY = Some("country-name"),
          AVERAGE_SELLING_PRICE = Some(12345.0),
          NUMBER_OF_COVERS = Some(150),
          NUMBER_OF_WEEKS_OPEN = Some(50),
          NUMBER_OF_DAYS_OPEN = Some(4),
          CONVENIENCE_LEVEL = Some("cooking-convenience-level"),
          RESPONSIBLE_EMPLOYEE = Some("sales-representative"),
          NPS_POTENTIAL = Some(BigDecimal(75.000000000000000000)),
          CAM_KEY = "",
          CAM_TEXT = "",
          CHANNEL_KEY = "",
          CHANNEL_TEXT = "",
          CHAIN_KNOTEN = Some("chain-id"),
          CHAIN_NAME = Some("chainname"),
          CUST_SUB_SEG_EXT = "",
          CUST_SEG_EXT = "",
          CUST_SEG_KEY_EXT = "",
          CUST_GRP_EXT = "",
          PARENT_SEGMENT = "",
          DATE_CREATED = defaultOperator.dateCreated,
          DATE_UPDATED = defaultOperator.dateUpdated,
          DELETE_FLAG = "N",
          WHOLESALER_OPERATOR_ID = None,
          PRIVATE_HOUSEHOLD = Some("N"),
          VAT = Some("vat"),
          OPEN_ON_MONDAY = Some("N"),
          OPEN_ON_TUESDAY = Some("Y"),
          OPEN_ON_WEDNESDAY = Some("Y"),
          OPEN_ON_THURSDAY = Some("Y"),
          OPEN_ON_FRIDAY = Some("Y"),
          OPEN_ON_SATURDAY = Some("Y"),
          OPEN_ON_SUNDAY = Some("N"),
          KITCHEN_TYPE = Some("kitchentype"),
          LOCAL_CHANNEL = Some("local-channel"),
          CHANNEL_USAGE = Some("channel-usage"),
          SOCIAL_COMMERCIAL = Some("social-commercial"),
          STRATEGIC_CHANNEL = Some("strategic-channel"),
          GLOBAL_CHANNEL = Some("global-channel"),
          GLOBAL_SUBCHANNEL = Some("global-sub-channel")
        )

      actualAcmOperator shouldBe expectedAcmOperator
    }
  }
}
