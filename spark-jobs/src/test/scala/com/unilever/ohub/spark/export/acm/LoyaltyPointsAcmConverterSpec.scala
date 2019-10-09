package com.unilever.ohub.spark.export.acm

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.entity.TestLoyaltyPoints
import com.unilever.ohub.spark.export.acm.model.AcmLoyaltyPoints
import org.scalatest.{FunSpec, Matchers}

class LoyaltyPointsAcmConverterSpec extends FunSpec with TestLoyaltyPoints with Matchers {

  private[acm] val SUT = LoyaltyPointsAcmConverter

  describe("Loyalty points acm converter") {
    it("should convert loyaly points correctly into acm loyalty points") {
      val loyaltyPoints = defaultLoyaltyPoints.copy(
        dateCreated = Some(Timestamp.valueOf("2015-06-30 13:47:00.0")),
        dateUpdated = Some(Timestamp.valueOf("2015-06-30 13:47:00.0")),
        contactPersonConcatId = Some("DE~EMAKINA~1234"),
        contactPersonOhubId = Some("b3a6208c-44e2-d7f6-80e2-f26d461f64c0")
      )
      val result = SUT.convert(loyaltyPoints)

      val expectedAcmLoyaltyPoints = AcmLoyaltyPoints(
        CP_ORIG_INTEGRATION_ID = "b3a6208c-44e2-d7f6-80e2-f26d461f64c0",
        COUNTRY_CODE = "DE",
        CP_LNKD_INTEGRATION_ID = "DE~EMAKINA~1234",
        EARNED = "15.00",
        SPENT = "15.00",
        ACTUAL = "15.00",
        GOAL = "15.00",
        UPDATED_AT = "2015/06/30 13:47:00",
        REWARD_NAME = "REWARD NAME",
        REWARD_IMAGE_URL = "imageUrl.png",
        REWARD_LDP_URL = "imagePageUrl.png",
        REWARD_EANCODE = "123456789"
        //LOYALTY_POINT_ID = "DE~EMAKINA~b3a6208c-d7f6-44e2-80e2-f26d461f64c0"
      )

      result shouldBe expectedAcmLoyaltyPoints
    }

    it("fill date updated with date created if date updated is null") {
      val lp = defaultLoyaltyPoints.copy(dateUpdated = Option.empty, dateCreated = Option(Timestamp.valueOf("2019-09-10 13:49:00.0")))
      val actualAcmLoyaltyPoints = SUT.convert(lp)
      assert(actualAcmLoyaltyPoints.UPDATED_AT equals ("2019/09/10 13:49:00"))
    }

  }
}
