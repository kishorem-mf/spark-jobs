package com.unilever.ohub.spark.export.dispatch

import com.unilever.ohub.spark.domain.entity.LoyaltyPoints
import com.unilever.ohub.spark.export.dispatch.model.DispatchLoyaltyPoints
import com.unilever.ohub.spark.export.{Converter, TransformationFunctions}

object LoyaltyPointsDispatcherConverter extends Converter[LoyaltyPoints, DispatchLoyaltyPoints] with TransformationFunctions with DispatchTransformationFunctions {

  override def convert(points: LoyaltyPoints): DispatchLoyaltyPoints = {
    DispatchLoyaltyPoints(
      CP_ORIG_INTEGRATION_ID = points.contactPersonConcatId,
      COUNTRY_CODE = points.countryCode,
      CP_LNKD_INTEGRATION_ID = points.contactPersonOhubId,
      EARNED = points.totalEarned,
      SPENT = points.totalSpent,
      ACTUAL = points.totalActual,
      GOAL = points.rewardGoal,
      UPDATED_AT = points.ohubUpdated
    )
  }
}
