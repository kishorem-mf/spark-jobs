package com.unilever.ohub.spark.domain.entity

import java.sql.Timestamp

object TestOrderLines extends TestOrderLines

trait TestOrderLines {

  // format: OFF
  lazy val defaultOrderLine: OrderLine = OrderLine(
    concatId = "country-code~source-name~source-entity-id",
    countryCode = "country-code",
    customerType = OrderLine.customerType,
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
    orderConcatId = None,
    productConcatId = None,
    quantityOfUnits= None,
    amount= None,
    pricePerUnit= None,
    currency= None,
    comment= None,
    // other fields
    additionalFields = Map(),
    ingestionErrors = Map()
  )
  // format: ON
}

