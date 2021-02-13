package com.unilever.ohub.spark.export.ddl

import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity.TestSubscription
import com.unilever.ohub.spark.export.ddl.model.DdlNewsletterSubscription

class NewsletterSubscriptionDdlConverterSpec extends SparkJobSpec with TestSubscription{
  val SUT = NewsletterSubscriptionDdlConverter
  val subscriptionToConvert = defaultSubscription.copy(contactPersonOhubId = Some("12345"))
  describe("subscription ddl converter") {
    it("should convert a domain subscription correctly into an ddl subscription") {
      val result = SUT.convert(subscriptionToConvert)
      val expectedNewsletterSubscription = DdlNewsletterSubscription(
        newsletterNumber = "",
        id = "b3a6208c-d7f6-44e2-80e2-f26d461f64c0",
        contactSAPID = "12345",
        createdBy = "",
        currency = "",
        deliveryMethod = "all",
        includePricing = "",
        language = "",
        newsletterId = "DE~EMAKINA~b3a6208c-d7f6-44e2-80e2-f26d461f64c0",
        newsletterName = "",
        owner = "",
        quantity = "",
        recordType = "",
        status = "",
        statusOptOut = "",
        subscribed = "Y",
        subscriptionConfirmed = "Y",
        subscriptionConfirmedDate = "2015-06-30 01:49:00:000",
        subscriptionDate = "2015-06-20 01:49:00:000",
        subscriptionType = "Newsletter",
        unsubscriptionConfirmedDate = "2015-06-30 01:49:00:000",
        unsubscriptionDate = "2015-06-20 01:49:00:000",
        comment = ""
      )
      result shouldBe expectedNewsletterSubscription
    }
  }

}
