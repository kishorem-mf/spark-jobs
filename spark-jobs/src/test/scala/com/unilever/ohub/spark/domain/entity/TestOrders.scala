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
    ohubCreated = Timestamp.valueOf("2015-06-30 13:49:00.0"),
    ohubUpdated = Timestamp.valueOf("2015-06-30 13:49:00.0"),
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
    transactionDate = Timestamp.valueOf("2015-06-30 13:49:00.0"),
    vat = None,
    // invoice address
    invoiceOperatorName                   = Some("invoiceOperatorName"),
    invoiceOperatorStreet                 = Some("invoiceOperatorStreet"),
    invoiceOperatorHouseNumber            = Some("invoiceOperatorHouseNumber"),
    invoiceOperatorHouseNumberExtension   = Some("invoiceOperatorHouseNumberExtension"),
    invoiceOperatorZipCode                = Some("invoiceOperatorZipCode"),
    invoiceOperatorCity                   = Some("invoiceOperatorCity"),
    invoiceOperatorState                  = Some("invoiceOperatorState"),
    invoiceOperatorCountry                = Some("invoiceOperatorCountry"),
    // delivery address
    deliveryOperatorName                  = Some("deliveryOperatorName"),
    deliveryOperatorStreet                = Some("deliveryOperatorStreet"),
    deliveryOperatorHouseNumber           = Some("deliveryOperatorHouseNumber"),
    deliveryOperatorHouseNumberExtension  = Some("deliveryOperatorHouseNumberExtension"),
    deliveryOperatorZipCode               = Some("deliveryOperatorZipCode"),
    deliveryOperatorCity                  = Some("deliveryOperatorCity"),
    deliveryOperatorState                 = Some("deliveryOperatorState"),
    deliveryOperatorCountry               = Some("deliveryOperatorCountry"),
    // other fields
    additionalFields = Map(),
    ingestionErrors = Map()
  )

  // format: ON
}