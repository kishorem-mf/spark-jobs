package com.unilever.ohub.spark.export.dispatch

import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity.TestCampaignOpens
import com.unilever.ohub.spark.export.dispatch.model.DispatchCampaignOpen

class CampaignOpenDispatchConverterSpec extends SparkJobSpec with TestCampaignOpens {

  val SUT = CampaignOpenDispatcherConverter

  describe("Campaign open dispatch converter") {
    it("should convert an open into an dispatch open") {
      val result = SUT.convert(defaultCampaignOpen)

      val expectedCampaignOpen = DispatchCampaignOpen(
        TRACKING_ID = "2691000",
        CAMPAIGN_WAVE_RESPONSE_ID = "6605058",
        COUNTRY_CODE = "NL",
        OPEN_DATE = "2016-04-17 18:00:30",
        CREATED_AT = "2015-06-30 13:49:00",
        UPDATED_AT = "2015-06-30 13:49:00"
      )

      result shouldBe expectedCampaignOpen
    }
  }
}
