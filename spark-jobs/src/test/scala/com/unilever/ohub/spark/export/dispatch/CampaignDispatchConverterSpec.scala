package com.unilever.ohub.spark.export.dispatch

import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity.TestCampaigns
import com.unilever.ohub.spark.export.dispatch.model.DispatchCampaign

class CampaignDispatchConverterSpec extends SparkJobSpec with TestCampaigns {

  val SUT = CampaignDispatcherConverter

  describe("Campaign open dispatch converter") {
    it("should convert an open into an dispatch open") {
      val result = SUT.convert(defaultCampaign)

      val expectedCampaign = DispatchCampaign(
        CP_ORIG_INTEGRATION_ID = "DE~OHUB~b3a6208",
        COUNTRY_CODE = "NL",
        CAMPAIGN_NAME = "20160324 - Lipton",
        CAMPAIGN_SPECIFICATION = "Product Introduction",
        CAMPAIGN_WAVE_START_DATE = "2017-09-28 13:49:00",
        CAMPAIGN_WAVE_END_DATE = "2017-09-28 13:50:00",
        CAMPAIGN_WAVE_STATUS = "ended",
        CONCAT_ID = "b3a6208c~NL~EMAKINA~1003499146~2018-10-08T22:53:51",
        CAMPAIGN_ID = "65054561",
        DELIVERY_ID = "65054561",
        CAMPAIGN_CONCAT_ID = "b3a6208c~NL~EMAKINA~1003499146",
        CREATED_AT= "2015-06-30 13:49:00",
        UPDATED_AT= "2015-06-30 13:49:00"
      )

      result shouldBe expectedCampaign
    }
  }
}
