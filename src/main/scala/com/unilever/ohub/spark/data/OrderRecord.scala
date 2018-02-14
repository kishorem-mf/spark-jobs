package com.unilever.ohub.spark.data

import java.sql.Timestamp

case class OrderRecord(
  orderConcatId: String,
  refOrderId: Option[String],
  source: Option[String],
  countryCode: Option[String],
  status: Option[Boolean],
  statusOriginal: Option[String],
  refOperatorId: Option[String],
  refContactPersonId: Option[String],
  orderType: Option[String],
  transactionDate: Option[Timestamp],
  transactionDateOriginal: Option[String],
  refProductId: Option[String],
  quantity: Option[Long],
  quantityOriginal: Option[String],
  orderLineValue: Option[BigDecimal],
  orderLineValueOriginal: Option[String],
  orderValue: Option[BigDecimal],
  orderValueOriginal: Option[String],
  wholesaler: Option[String],
  campaignCode: Option[String],
  campaignName: Option[String],
  unitPrice: Option[BigDecimal],
  unitPriceOriginal: Option[String],
  currencyCode: Option[String],
  dateCreated: Option[Timestamp],
  dateModified: Option[Timestamp]
)
