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
    sourceEntityId = "source-entity-id",
    sourceName = "source-name",
    ohubId = Some("ohub-id"),
    ohubCreated = new Timestamp(System.currentTimeMillis()),
    ohubUpdated = new Timestamp(System.currentTimeMillis()),
    // specific fields
    orderConcatId = "",
    productConcatId = "product-concat-id",
    quantityOfUnits= 0,
    amount= BigDecimal(0),
    pricePerUnit= None,
    currency= None,
    comment= None,
    // other fields
    additionalFields = Map(),
    ingestionErrors = Map()
  )
  // format: ON
}

