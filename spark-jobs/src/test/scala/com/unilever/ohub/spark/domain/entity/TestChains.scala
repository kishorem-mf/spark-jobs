package com.unilever.ohub.spark.domain.entity

import java.sql.Timestamp

trait TestChains {

  lazy val defaultChains = Chain(
    id = "id-1",
    creationTimestamp = new Timestamp(1542205922011L),
    concatId = "DE~FAIRKITCHENS~b3a6208c-d7f6-44e2-80e2-f26d461f64c0",
    countryCode = "DE",
    //customerType = "CHAIN",
    dateCreated = None,
    dateUpdated = None,
    isActive = true,
    isGoldenRecord = false,
    sourceEntityId = "b3a6208c-d7f6-44e2-80e2-f26d461f64c0",
    sourceName = "FAIRKITCHENS",
    ohubId = None,
    ohubCreated = Timestamp.valueOf("2019-07-30 13:49:00.0"),
    ohubUpdated = Timestamp.valueOf("2019-07-30 13:49:00.0"),

    conceptName = Some("newPromo"),
    numberOfUnits = Some(23),
    numberOfStates = Some(233),
    estimatedAnnualSales = Some(33.23),
    estimatedPurchasePotential = Some(23.93),
    address = Some("33 weena"),
    city = Some("Rotterdam"),
    state = None,
    zipCode = None,
    website = Some("www.google.com"),
    phone = None,
    segment = None,
    primaryMenu = Some("sause"),
    secondaryMenu = None,

    additionalFields = Map(),
    ingestionErrors = Map()
  )

}
