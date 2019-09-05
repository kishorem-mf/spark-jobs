package com.unilever.ohub.spark.export.dispatch

import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity.TestOperators
import com.unilever.ohub.spark.export.dispatch.model.DispatchOperator

class OperatorDispatchConverterSpec extends SparkJobSpec with TestOperators {

  val SUT = OperatorDispatchConverter

  describe("operator dispatch converter") {
    it("should convert a domain operator correctly into an dispatch operator") {

      val operatorToConvert = defaultOperator.copy(
        concatId = "AU~WUFOO~AB123",
        localChannel = Some("local-channel"),
        channelUsage = Some("channel-usage"),
        socialCommercial = Some("social-commercial"),
        strategicChannel = Some("strategic-channel"),
        globalChannel = Some("global-channel"),
        globalSubChannel = Some("global-sub-channel"),
        hasEmailOptOut = None,
        hasTelemarketingOptOut = Some(true)
      )
      val result = SUT.convert(operatorToConvert)

      val expectedDispatchOperator = DispatchOperator(
          COUNTRY_CODE= "country-code",
          OPR_ORIG_INTEGRATION_ID= "AU~WUFOO~AB123",
          OPR_LNKD_INTEGRATION_ID= operatorToConvert.ohubId.get,
          GOLDEN_RECORD_FLAG= "N",
          SOURCE= "source-name",
          SOURCE_ID= "source-entity-id",
          CREATED_AT= "2017-10-16 18:09:49",
          UPDATED_AT= "2017-10-16 18:09:49",
          DELETE_FLAG= "N",
          NAME= "operatorname",
          CHANNEL= "channel",
          SUB_CHANNEL= "sub-channel",
          REGION= "region",
          OTM= "D",
          PREFERRED_PARTNER= "distributorname",
          STREET= "street",
          HOUSE_NUMBER= "12",
          HOUSE_NUMBER_EXT= "",
          CITY= "city",
          COUNTRY= "country-name",
          ZIP_CODE= "1234 AB",
          AVERAGE_SELLING_PRICE= "12345.00",
          NUMBER_OF_COVERS= "150",
          NUMBER_OF_WEEKS_OPEN= "50",
          NUMBER_OF_DAYS_OPEN= "4",
          STATUS= "A",
          CONVENIENCE_LEVEL= "cooking-convenience-level",
          RESPONSIBLE_EMPLOYEE= "sales-representative",
          NPS_POTENTIAL= "75.00",
          CHANNEL_TEXT= "channel",
          CHAIN_KNOTEN= "chain-id",
          CHAIN_NAME= "chainname",
          DM_OPT_OUT= "N",
          EMAIL_OPT_OUT= "U",
          FIXED_OPT_OUT= "Y",
          MOBILE_OPT_OUT= "N",
          FAX_OPT_OUT= "N",
          KITCHEN_TYPE= "kitchentype",
          STATE= "state",
          WHOLE_SALER_OPERATOR_ID= "",
          PRIVATE_HOUSEHOLD= "N",
          VAT= "vat",
          OPEN_ON_MONDAY= "0",
          OPEN_ON_TUESDAY= "1",
          OPEN_ON_WEDNESDAY= "1",
          OPEN_ON_THURSDAY= "1",
          OPEN_ON_FRIDAY= "1",
          OPEN_ON_SATURDAY= "1",
          OPEN_ON_SUNDAY= "0",
          LOCAL_CHANNEL= "local-channel",
          CHANNEL_USAGE= "channel-usage",
          SOCIAL_COMMERCIAL= "social-commercial",
          STRATEGIC_CHANNEL= "strategic-channel",
          GLOBAL_CHANNEL= "global-channel",
          GLOBAL_SUBCHANNEL= "global-sub-channel")

      result shouldBe expectedDispatchOperator
    }
  }

}
