package com.unilever.ohub.spark.export.dispatch

import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity.TestCampaignSends
import com.unilever.ohub.spark.export.dispatch.model.DispatchCampaignSend

class CampaignSendDispatchConverterSpec extends SparkJobSpec with TestCampaignSends {

  val SUT = CampaignSendDispatcherConverter

  describe("Campaign send dispatch converter") {
    it("should convert a send into an dispatch send") {
      val result = SUT.convert(defaultCampaignSend)

      val expectedCampaignSend = DispatchCampaignSend(
        CP_ORIG_INTEGRATION_ID = "NL~1003499146~10037~25006~ULNL~3~4",
        CWS_INTEGRATION_ID = "b3a6208c~NL~EMAKINA~1003499146~2018-10-08T22:53:51",
        CWS_DATE = "2016-03-28 21:16:50",
        COUNTRY_CODE = "NL",
        CAMPAIGN_NAME = "20160324 - Lipton",
        WAVE_NAME = "20160324 - Lipton~NLLipton032016 -- 20160323 -- proofed",
        CHANNEL = "Email",
        CAMPAIGN_WAVE_RESPONSE_ID = "6160583",
        CONTROL_POPULATION = "0",
        PROOF_GROUP_MEMBER = "1",
        SELECTION_FOR_OFFLINE_CHANNELS = "24",
        CAMPAIGN_ID = "65054561",
        DELIVERY_ID = "65096985",
        CAMPAIGN_CONCAT_ID = "b3a6208c~NL~EMAKINA~1003499146~f26d461f64c0"
      )

      result shouldBe expectedCampaignSend
    }
  }
}
