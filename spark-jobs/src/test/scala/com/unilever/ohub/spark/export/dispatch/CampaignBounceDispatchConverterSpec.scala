package com.unilever.ohub.spark.export.dispatch

import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity.TestCampaignBounces
import com.unilever.ohub.spark.export.dispatch.model.DispatchCampaignBounce

class CampaignBounceDispatchConverterSpec extends SparkJobSpec with TestCampaignBounces {

  val SUT = CampaignBounceDispatcherConverter

  describe("Campaign bounce dispatch converter") {
    it("should convert a bounce into an dispatch bounce") {
      val result = SUT.convert(defaultCampaignBounce)

      val expectedCampaignBounce = DispatchCampaignBounce(
        CAMPAIGN_WAVE_RESPONSE_ID= "6605058",
        COUNTRY_CODE= "NL",
        BOUNCE_DATE= "2016-04-17 18:00:30",
        FAILURE_TYPE= "Hard",
        FAILURE_REASON= "User unknown"
      )

      result shouldBe expectedCampaignBounce
    }
  }
}
