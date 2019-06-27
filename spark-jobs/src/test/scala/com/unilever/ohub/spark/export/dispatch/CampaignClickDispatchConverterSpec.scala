package com.unilever.ohub.spark.export.dispatch

import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity.TestCampaignClicks
import com.unilever.ohub.spark.export.dispatch.model.DispatchCampaignClick

class CampaignClickDispatchConverterSpec extends SparkJobSpec with TestCampaignClicks {

  val SUT = CampaignClickDispatcherConverter

  describe("Campaign click dispatch converter") {
    it("should convert a click into an dispatch click") {
      val result = SUT.convert(defaultCampaignClick)

      val expectedCampaignClick = DispatchCampaignClick(
        TRACKING_ID= "2695121",
        CAMPAIGN_WAVE_RESPONSE_ID= "6605058",
        COUNTRY_CODE= "NL",
        CLICK_DATE= "2015-06-30 13:49:00",
        CLICKED_URL= "https://www.dasgrossestechen.at/gewinnspiel",
        MOBILE_DEVICE= "false",
        OPERATING_SYSTEM= "",
        BROWSER_NAME= "",
        BROWSER_VERSION= ""
      )

      result shouldBe expectedCampaignClick
    }
  }
}
