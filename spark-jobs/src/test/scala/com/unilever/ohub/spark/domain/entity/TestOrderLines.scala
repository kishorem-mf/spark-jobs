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
    ohubCreated = Timestamp.valueOf("2015-06-30 13:49:00.0"),
    ohubUpdated = Timestamp.valueOf("2015-06-30 13:49:00.0"),
    // specific fields
    orderConcatId = "",
    productConcatId = "product-concat-id",
    productSourceEntityId = "product-source-entity-id",
    quantityOfUnits= 0,
    amount = BigDecimal(0),
    pricePerUnit = None,
    currency = None,
    comment = None,
    campaignLabel = Some("campaign-label"),
    loyaltyPoints = Some(123),
    productOhubId = Some("product-ohub-id"),
    // other fields
    additionalFields = Map(),
    ingestionErrors = Map()
  )
  // format: ON
}

