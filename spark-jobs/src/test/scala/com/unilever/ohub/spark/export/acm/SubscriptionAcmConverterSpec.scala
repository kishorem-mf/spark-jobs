package com.unilever.ohub.spark.export.acm

import com.unilever.ohub.spark.domain.entity.TestSubscription
import com.unilever.ohub.spark.export.acm.model.AcmSubscription
import org.scalatest.{FunSpec, Matchers}

class SubscriptionAcmConverterSpec extends FunSpec with TestSubscription with Matchers {

  private[acm] val SUT = SubscriptionAcmConverter

  describe("Subscription acm converter") {
    it("should convert a subscription correctly into an acm subscription") {
      val subscription = defaultSubscription.copy(contactPersonOhubId = Some("DE~EMAKINA~1234"))
      val result = SUT.convert(subscription)

      val expectedAcmSubscription = AcmSubscription(
        CONTACT_PARTY_ID = "DE~EMAKINA~1234",
        COUNTRY_CODE = "DE",
        SUBSCRIBE_FLAG = "Y",
        SERVICE_NAME = "Newsletter",
        DATE_CREATED = "2015/06/30 13:49:00",
        DATE_UPDATED = "2015/06/30 13:49:00",
        SUBSCRIPTION_DATE = "2015/06/20 13:49:00",
        SUBSCRIPTION_CONFIRMED = "Y",
        SUBSCRIPTION_CONFIRMED_DATE = "2015/06/30 13:49:00",
        FAIR_KITCHENS_SIGN_UP_TYPE = "pledger"
      )

      result shouldBe expectedAcmSubscription
    }
  }
}
