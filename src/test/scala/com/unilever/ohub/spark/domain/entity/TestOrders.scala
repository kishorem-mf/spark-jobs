package com.unilever.ohub.spark.domain.entity

import java.sql.Timestamp

object TestOrders extends TestOrders

trait TestOrders {

  // format: OFF
  lazy val defaultOrder: Order = Order(
    concatId = "country-code~source-name~source-entity-id",
    countryCode = "country-code",
    customerType = Order.customerType,
    dateCreated = None,
    dateUpdated = None,
    isActive = true,
    isGoldenRecord = true,
    ohubId = Some("ohub-id"),
    sourceEntityId = "source-entity-id",
    sourceName = "source-name",
    ohubCreated = new Timestamp(System.currentTimeMillis()),
    ohubUpdated = new Timestamp(System.currentTimeMillis()),
    // specific fields
    `type` = "DIRECT",
    campaignCode = Some("UNKNOWN"),
    campaignName = Some("campaign"),
    comment = None,
    contactPersonConcatId = Some("some~contact~person"),
    contactPersonOhubId = None,
    distributorId = None,
    distributorLocation = None,
    distributorName = Some("Van der Valk"),
    distributorOperatorId = None,
    operatorConcatId = "some~operator~id",
    operatorOhubId = None,
    transactionDate = new Timestamp(System.currentTimeMillis()),
    vat = None,
    // other fields
    additionalFields = Map(),
    ingestionErrors = Map()
  )

  // format: ON
}

