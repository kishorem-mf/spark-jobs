package com.unilever.ohub.spark.export.acm

import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity.TestOperators
import com.unilever.ohub.spark.export.acm.model.AcmOperator

class OperatorAcmConverterSpec extends SparkJobSpec with TestOperators {

  private[acm] val SUT = OperatorAcmConverter

  describe("operator acm converter") {
    it("should convert a domain operator correctly into an acm converter") {

      val operatorToConvert = defaultOperator.copy(
        concatId = "AU~WUFOO~AB123",
        localChannel = Some("local-channel"),
        channelUsage = Some("channel-usage"),
        socialCommercial = Some("social-commercial"),
        strategicChannel = Some("strategic-channel"),
        globalChannel = Some("global-channel"),
        globalSubChannel = Some("global-sub-channel")
      )
      val result = SUT.convert(operatorToConvert)

      val expectedAcmOperator =
        AcmOperator(
          OPR_ORIG_INTEGRATION_ID = defaultOperator.ohubId.get,
          OPR_LNKD_INTEGRATION_ID = "AU~AB123~1~19",
          GOLDEN_RECORD_FLAG = "N",
          COUNTRY_CODE = "country-code",
          NAME = "operatorname",
          CHANNEL = "channel",
          SUB_CHANNEL = "sub-channel",
          ROUTE_TO_MARKET = "",
          REGION = "region",
          OTM = "D",
          PREFERRED_PARTNER = "distributorname",
          STREET = "street",
          HOUSE_NUMBER = "12",
          ZIPCODE = "1234 AB",
          CITY = "city",
          COUNTRY = "country-name",
          AVERAGE_SELLING_PRICE = "12345.00",
          NUMBER_OF_COVERS = "150",
          NUMBER_OF_WEEKS_OPEN = "50",
          NUMBER_OF_DAYS_OPEN = "4",
          CONVENIENCE_LEVEL = "cooking-convenience-level",
          RESPONSIBLE_EMPLOYEE = "sales-representative",
          NPS_POTENTIAL = "75.00",
          CAM_KEY = "",
          CAM_TEXT = "",
          CHANNEL_KEY = "",
          CHANNEL_TEXT = "",
          CHAIN_KNOTEN = "chain-id",
          CHAIN_NAME = "chainname",
          CUST_SUB_SEG_EXT = "",
          CUST_SEG_EXT = "",
          CUST_SEG_KEY_EXT = "",
          CUST_GRP_EXT = "",
          PARENT_SEGMENT = "",
          DATE_CREATED = "2017/10/16 18:09:49",
          DATE_UPDATED = "2017/10/16 18:09:49",
          DELETE_FLAG = "N",
          WHOLESALER_OPERATOR_ID = "",
          PRIVATE_HOUSEHOLD = "N",
          VAT = "vat",
          OPEN_ON_MONDAY = "N",
          OPEN_ON_TUESDAY = "Y",
          OPEN_ON_WEDNESDAY = "Y",
          OPEN_ON_THURSDAY = "Y",
          OPEN_ON_FRIDAY = "Y",
          OPEN_ON_SATURDAY = "Y",
          OPEN_ON_SUNDAY = "N",
          KITCHEN_TYPE = "kitchentype",
          LOCAL_CHANNEL = "local-channel",
          CHANNEL_USAGE = ("channel-usage"),
          SOCIAL_COMMERCIAL = ("social-commercial"),
          STRATEGIC_CHANNEL = ("strategic-channel"),
          GLOBAL_CHANNEL = ("global-channel"),
          GLOBAL_SUBCHANNEL = ("global-sub-channel")
        )

      result shouldBe expectedAcmOperator
    }
  }

}
