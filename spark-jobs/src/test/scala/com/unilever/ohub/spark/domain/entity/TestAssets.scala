package com.unilever.ohub.spark.domain.entity

import java.sql.Timestamp

object TestAssets extends TestAssets

trait TestAssets {

  lazy val defaultAsset: Asset = Asset(
    id = "id-1",
    creationTimestamp = new Timestamp(1542205922011L),
    concatId = "DE~EMAKINA~b3a6208c-d7f6-44e2-80e2-f26d461f64c0",
    countryCode = "DE",
    customerType = "CONTACTPERSON",
    isActive = true,
    sourceEntityId = "b3a6208c-d7f6-44e2-80e2-f26d461f64c0",
    sourceName = "EMAKINA",
    ohubCreated = Timestamp.valueOf("2015-06-30 13:49:00.0"),
    ohubUpdated = Timestamp.valueOf("2015-06-30 13:49:00.0"),
    dateCreated = None,
    dateUpdated = None,
    ohubId = null,
    isGoldenRecord = false,

    crmId = None,
    name =  None,
    brandName =  None,
    `type` = None,
    description =  None,
    dimensions =  None,
    numberOfTimesRepaired =  None,
    powerConsumption =  None,
    numberOfCabinetBaskets =  None,
    numberOfCabinetFacings =  None,
    serialNumber =  None,
    oohClassification =  None,
    lifecyclePhase =  None,
    capacityInLiters =  None,
    dateCreatedInUdl =  None,
    leasedOrSold =  None,
    crmTaskId =  None,

    additionalFields = Map(),
    ingestionErrors = Map()
  )
}
