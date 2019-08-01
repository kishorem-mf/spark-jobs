package com.unilever.ohub.spark.export.acm

import com.unilever.ohub.spark.domain.entity.TestSubscription
import com.unilever.ohub.spark.export.acm.model.AcmSubscription
import org.scalatest.{FunSpec, Matchers}

class SubscriptionAcmConverterSpec extends FunSpec with TestSubscription with Matchers {

  private[acm] val SUT = SubscriptionAcmConverter

  describe("Subscription acm converter") {
    it("should convert a subscription correctly into an acm subscription") {
      val subscription = defaultSubscription.copy(contactPersonOhubId = Some("a3a6208c-d7f6-44e2-80e2-f26d461f64c0"))
      val result = SUT.convert(subscription)

      val expectedAcmSubscription = AcmSubscription(
        SUBSCRIPTION_ID = "DE~EMAKINA~b3a6208c-d7f6-44e2-80e2-f26d461f64c0",
        CP_LNKD_INTEGRATION_ID = "a3a6208c-d7f6-44e2-80e2-f26d461f64c0",
        COUNTRY_CODE = "DE",
        SUBSCRIBE_FLAG = "Y",
        DATE_CREATED = "2015/06/30 13:49:00",
        DATE_UPDATED = "2015/06/30 13:49:00",
        SUBSCRIPTION_DATE = "2015/06/20 13:49:00",
        SUBSCRIPTION_CONFIRMED = "Y",
        SUBSCRIPTION_CONFIRMED_DATE = "2015/06/30 13:49:00",
        FAIR_KITCHENS_SIGN_UP_TYPE = "pledger",
        COMMUNICATION_CHANNEL = "all",
        SUBSCRIPTION_TYPE = "Newsletter",
        DELETED_FLAG = "N"
      )

      result shouldBe expectedAcmSubscription
    }
  }
}
