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
        CP_ORIG_INTEGRATION_ID= "DE~FILE~138175",
        COUNTRY_CODE= "DE",
        CP_LNKD_INTEGRATION_ID= "",
        EARNED= "15.00",
        SPENT= "15.00",
        ACTUAL= "15.00",
        GOAL= "15.00",
        UPDATED_AT= "2015-06-30 13:49:00"
      )

      result shouldBe expectedLoyaltyPoints
    }
  }
}
