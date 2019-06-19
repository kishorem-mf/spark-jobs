package com.unilever.ohub.spark.export.acm

import com.unilever.ohub.spark.domain.entity.LoyaltyPoints
import com.unilever.ohub.spark.export.acm.model.AcmLoyaltyPoints
import com.unilever.ohub.spark.export.{Converter, TransformationFunctions}

object LoyaltyPointsAcmConverter extends Converter[LoyaltyPoints, AcmLoyaltyPoints] with TransformationFunctions with AcmTransformationFunctions {

  override def convert(loyaltyPoints: LoyaltyPoints): AcmLoyaltyPoints = {
    model.AcmLoyaltyPoints(
      CP_ORIG_INTEGRATION_ID = loyaltyPoints.contactPersonOhubId,
      COUNTRY_CODE = loyaltyPoints.countryCode,
      CP_LNKD_INTEGRATION_ID = loyaltyPoints.contactPersonConcatId,
      EARNED = loyaltyPoints.totalEarned,
      SPENT = loyaltyPoints.totalSpent,
      ACTUAL = loyaltyPoints.totalActual,
      GOAL = loyaltyPoints.rewardGoal,
      UPDATED_AT = loyaltyPoints.ohubUpdated,
      REWARD_NAME = loyaltyPoints.rewardName,
      REWARD_IMAGE_URL = loyaltyPoints.rewardImageUrl,
      REWARD_LANDING_PAGE_URL = loyaltyPoints.rewardLandingPageUrl,
      REWARD_EAN_CODE = loyaltyPoints.rewardEanCode
    )
  }
}
