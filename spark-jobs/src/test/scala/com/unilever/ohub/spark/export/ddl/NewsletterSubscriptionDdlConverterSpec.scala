package com.unilever.ohub.spark.export.ddl

import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity.TestSubscription
import com.unilever.ohub.spark.export.ddl.model.DdlNewsletterSubscription

class NewsletterSubscriptionDdlConverterSpec extends SparkJobSpec with TestSubscription {
  val SUT = NewsletterSubscriptionDdlConverter
  val subscriptionToConvert = defaultSubscription.copy(contactPersonOhubId = Some("12345"))
  describe("subscription ddl converter") {
    it("should convert a domain subscription correctly into an ddl subscription") {
      val result = SUT.convert(subscriptionToConvert)
      val expectedNewsletterSubscription = DdlNewsletterSubscription(
        `Newsletter Number` = "",
        ID = "b3a6208c-d7f6-44e2-80e2-f26d461f64c0",
        `Contact SAP ID` = "12345",
        `Created By` = "",
        Currency = "",
        `Delivery Method` = "all",
        `Include Pricing` = "",
        Language = "",
        `Newsletter ID` = "DE~EMAKINA~b3a6208c-d7f6-44e2-80e2-f26d461f64c0",
        `Newsletter Name` = "",
        Owner = "",
        Quantity = "",
        `Record Type` = "",
        Status = "",
        `Status Opt Out` = "",
        Subscribed = "Y",
        `Subscription Confirmed` = "Y",
        `Subscription Confirmed Date` = "2015-06-30 01:49:00:000",
        `Subscription Date` = "2015-06-20 01:49:00:000",
        `Subscription Type` = "Newsletter",
        `Unsubscription Confirmed Date` = "2015-06-30 01:49:00:000",
        `Unsubscription Date` = "2015-06-20 01:49:00:000",
        Comment = ""
      )
      result shouldBe expectedNewsletterSubscription
    }
  }

}
