package com.unilever.ohub.spark.export.acm

import com.unilever.ohub.spark.domain.entity.LoyaltyPoints
import com.unilever.ohub.spark.export.acm.model.AcmLoyaltyPoints
import com.unilever.ohub.spark.export.{Converter, TypeConversionFunctions}

object LoyaltyPointsAcmConverter extends Converter[LoyaltyPoints, AcmLoyaltyPoints] with TypeConversionFunctions with AcmTransformationFunctions {

  override def convert(loyaltyPoints: LoyaltyPoints): AcmLoyaltyPoints = {
    implicit val lp = loyaltyPoints

    model.AcmLoyaltyPoints(
      CP_ORIG_INTEGRATION_ID = getValue("contactPersonOhubId"),
      COUNTRY_CODE = getValue("countryCode"),
      CP_LNKD_INTEGRATION_ID = getValue("contactPersonConcatId"),
      EARNED = getValue("totalEarned"),
      SPENT = getValue("totalSpent"),
      ACTUAL = getValue("totalActual"),
      GOAL = getValue("rewardGoal"),
      UPDATED_AT = getValue("ohubUpdated"),
      REWARD_NAME = getValue("rewardName"),
      REWARD_IMAGE_URL = getValue("rewardImageUrl"),
      REWARD_LDP_URL = getValue("rewardLandingPageUrl"),
      REWARD_EANCODE = getValue("rewardEanCode")
      //LOYALTY_POINT_ID = getValue("concatId"),
    )
  }
}
