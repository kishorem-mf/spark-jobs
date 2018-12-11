package com.unilever.ohub.spark.domain.entity

import java.sql.Timestamp

trait TestLoyaltyPoints {

  lazy val defaultLoyaltyPoints: LoyaltyPoints = LoyaltyPoints(
    id = "id-1",
    creationTimestamp = new Timestamp(1542205922011L),
    concatId = "DE~EMAKINA~b3a6208c-d7f6-44e2-80e2-f26d461f64c0",
    countryCode = "DE",
    customerType = "CONTACTPERSON",
    dateCreated = None,
    dateUpdated = None,
    isActive = true,
    isGoldenRecord = false,
    sourceEntityId = "b3a6208c-d7f6-44e2-80e2-f26d461f64c0",
    sourceName = "EMAKINA",
    ohubId = null,
    ohubCreated = Timestamp.valueOf("2015-06-30 13:49:00.0"),
    ohubUpdated = Timestamp.valueOf("2015-06-30 13:49:00.0"),

    totalLoyaltyPointsEarned = Some(BigDecimal.apply(15)),
    totalLoyaltyPointsSpent = Some(BigDecimal.apply(15)),
    totalLoyaltyPointsActual = Some(BigDecimal.apply(15)),
    loyaltyRewardGoal = Some(BigDecimal.apply(15)),

    contactPersonRefId = None,
    contactPersonConcatId = Some("DE~FILE~138175"),
    contactPersonOhubId = None,
    operatorRefId = None,
    operatorConcatId = Some("DE~FILE~138175"),
    operatorOhubId = None,

    additionalFields = Map(),
    ingestionErrors = Map()
  )
}
