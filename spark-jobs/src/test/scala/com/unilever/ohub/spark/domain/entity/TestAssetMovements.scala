package com.unilever.ohub.spark.domain.entity

import java.sql.Timestamp

object TestAssetMovements extends TestAssetMovements

trait TestAssetMovements {

  lazy val defaultAssetMovement: AssetMovement = AssetMovement(
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
    operatorOhubId = None,
    assemblyDate = None,
    assetConcatId = None,
    createdBy = None,
    currency = None,
    deliveryDate = None,
    lastModifiedBy = None,
    name = None,
    comment = None,
    owner = None,
    quantityOfUnits = None,
    returnDate = None,
    assetStatus = None,

    additionalFields = Map(),
    ingestionErrors = Map()
  )
}
