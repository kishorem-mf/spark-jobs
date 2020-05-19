package com.unilever.ohub.spark.export.dispatch

import com.unilever.ohub.spark.domain.entity.LoyaltyPoints
import com.unilever.ohub.spark.export.dispatch.model.DispatchLoyaltyPoints
import com.unilever.ohub.spark.export.{Converter, BooleanToYNConverter, InvertedBooleanToYNConverter, TypeConversionFunctions}

object LoyaltyPointsDispatcherConverter extends Converter[LoyaltyPoints, DispatchLoyaltyPoints] with TypeConversionFunctions with DispatchTypeConversionFunctions {

  override def convert(implicit points: LoyaltyPoints, explain: Boolean = false): DispatchLoyaltyPoints = {
    DispatchLoyaltyPoints(
      LOY_ORIG_INTEGRATION_ID = getValue("concatId"),
      CP_ORIG_INTEGRATION_ID = getValue("contactPersonConcatId"),
      CP_LNKD_INTEGRATION_ID = getValue("contactPersonOhubId"),
      COUNTRY_CODE = getValue("countryCode"),
      DELETE_FLAG = getValue("isActive", InvertedBooleanToYNConverter),
      GOLDEN_RECORD_FLAG = getValue("isGoldenRecord", BooleanToYNConverter),
      CREATED_AT = getValue("ohubCreated"),
      UPDATED_AT = getValue("ohubUpdated"),
      OPR_ORIG_INTEGRATION_ID = getValue("operatorConcatId"),
      OPR_LNKD_INTEGRATION_ID = getValue("operatorOhubId"),
      EARNED = getValue("totalEarned"),
      SPENT = getValue("totalSpent"),
      ACTUAL = getValue("totalActual"),
      GOAL = getValue("rewardGoal"),
      NAME = getValue("rewardName"),
      IMAGE_URL = getValue("rewardImageUrl"),
      LANDING_PAGE_URL = getValue("rewardLandingPageUrl"),
      EAN_CODE = getValue("rewardEanCode")
    )
  }
}
