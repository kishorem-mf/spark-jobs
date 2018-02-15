package com.unilever.ohub.spark.data

  case class EmWebOrderItemRecord(
    countryCode: Option[String],
    amount: Option[Long],
    unitPrice: Option[Long],
    itemUnitPrice: Option[Long],
    loyaltyPoints: Option[Long],
  )
