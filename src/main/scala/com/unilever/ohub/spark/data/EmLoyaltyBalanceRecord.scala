package com.unilever.ohub.spark.data

  case class EmLoyaltyBalanceRecord(
    countryCode: Option[String],
    totalLoyaltyPointsEarned: Option[Long],
    totalLoyaltyPointsSpent: Option[Long],
    totalLoyaltyPointsActual: Option[Long],
    totalLoyaltyPointsGoal: Option[Long])
