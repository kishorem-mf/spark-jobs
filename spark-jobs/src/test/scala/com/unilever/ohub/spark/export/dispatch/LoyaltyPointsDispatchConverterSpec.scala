package com.unilever.ohub.spark.export.dispatch

import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity.TestLoyaltyPoints
import com.unilever.ohub.spark.export.dispatch.model.DispatchLoyaltyPoints

class LoyaltyPointsDispatchConverterSpec extends SparkJobSpec with TestLoyaltyPoints {

  val SUT = LoyaltyPointsDispatcherConverter

  describe("Loyalty points dispatch converter") {
    it("should convert loyalty points into an dispatch loyalty points") {
      val result = SUT.convert(defaultLoyaltyPoints)

      val expectedLoyaltyPoints = DispatchLoyaltyPoints(
        LOY_ORIG_INTEGRATION_ID= "DE~EMAKINA~b3a6208c-d7f6-44e2-80e2-f26d461f64c0",
        CP_ORIG_INTEGRATION_ID= "DE~FILE~138175",
        CP_LNKD_INTEGRATION_ID= "",
        COUNTRY_CODE= "DE",
        DELETE_FLAG = "N",
        GOLDEN_RECORD_FLAG = "N",
        CREATED_AT= "2015-06-30 13:49:00",
        UPDATED_AT= "2015-06-30 13:49:00",
        OPR_ORIG_INTEGRATION_ID="DE~FILE~138175",
        OPR_LNKD_INTEGRATION_ID="",
        EARNED= "15.00",
        SPENT= "15.00",
        ACTUAL= "15.00",
        GOAL= "15.00",
        NAME="REWARD NAME",
        IMAGE_URL="imageUrl.png",
        LANDING_PAGE_URL="imagePageUrl.png",
        EAN_CODE="123456789"
      )

      result shouldBe expectedLoyaltyPoints
    }
  }
}
