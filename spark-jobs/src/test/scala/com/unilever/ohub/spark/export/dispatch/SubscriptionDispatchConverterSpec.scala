package com.unilever.ohub.spark.export.dispatch

import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity.TestSubscription
import com.unilever.ohub.spark.export.dispatch.model.DispatchSubscription

class SubscriptionDispatchConverterSpec extends SparkJobSpec with TestSubscription {

  val SUT = SubscriptionDispatchConverter

  describe("Subscription dispatch converter") {
    it("should convert a subscription into an dispatch subscription") {
      val result = SUT.convert(defaultSubscription)

      val expectedOrder = DispatchSubscription(
        CP_ORIG_INTEGRATION_ID = "DE~SUBSCRIPTION~138175",
        SUBSCR_INTEGRATION_ID = "DE~EMAKINA~b3a6208c-d7f6-44e2-80e2-f26d461f64c0",
        COUNTRY_CODE = "DE",
        CREATED_AT = "2015-06-30 13:49:00",
        UPDATED_AT = "2015-06-30 13:49:00",
        DELETE_FLAG = "N",
        NL_NAME = "Newsletter",
        REGION = "DE",
        SUBSCRIBED = "1",
        SUBSCRIPTION_DATE = "2015-06-20 13:49:00",
        SUBSCRIPTION_CONFIRMED = "1",
        SUBSCRIPTION_CONFIRMED_DATE = "2015-06-30 13:49:00"
      )

      result shouldBe expectedOrder
    }
  }
}
