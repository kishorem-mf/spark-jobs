package com.unilever.ohub.spark.data

  case class EmOrderItemRecord(
    orderLineId: Option[Long],
    countryCode: Option[String], // enum
    refOrderLineId: Option[Boolean], // 0/1
    orderFk: Option[Long],
    quantity: Option[Boolean], // 1
    amount: Option[Boolean], // 0
    unitPrice: Option[Boolean], // 0
    unitPriceCurrency: Option[String], // empty
    itemId: Option[Long],
    itemType: Option[String], // Sample
    itemName: Option[String],
    eanCustomerUnit: Option[String], // ?, empty
    eanDispatchUnit: Option[String], // ?, empty
    mrdrCode: Option[String], // ?, empty
    category: Option[String], // ?, empty
    subCategory: Option[String], // ?, empty
    brand: Option[String], // ?, empty
    subBrand: Option[String], // ?, empty
    itemUnit: Option[String], // ?, empty
    itemUnitPrice: Option[Long], // 0
    itemUnitPriceCurrency: Option[String], // ?, empty
    loyaltyPoints: Option[String], // ?, empty
    campaignLabel: Option[String], // ?, empty
    comments: Option[String]) // ?, empty
