package com.unilever.ohub.spark.export.dispatch

import com.unilever.ohub.spark.domain.entity.LoyaltyPoints
import com.unilever.ohub.spark.export.dispatch.model.DispatchLoyaltyPoints
import com.unilever.ohub.spark.export.{Converter, TypeConversionFunctions}

object LoyaltyPointsDispatcherConverter extends Converter[LoyaltyPoints, DispatchLoyaltyPoints] with TypeConversionFunctions with DispatchTypeConversionFunctions {

  override def convert(implicit points: LoyaltyPoints, explain: Boolean = false): DispatchLoyaltyPoints = {
    DispatchLoyaltyPoints(
      CP_ORIG_INTEGRATION_ID = getValue("contactPersonConcatId"),
      COUNTRY_CODE = getValue("countryCode"),
      CP_LNKD_INTEGRATION_ID = getValue("contactPersonOhubId"),
      EARNED = getValue("totalEarned"),
      SPENT = getValue("totalSpent"),
      ACTUAL = getValue("totalActual"),
      GOAL = getValue("rewardGoal"),
      UPDATED_AT = getValue("ohubUpdated")
    )
  }
}
