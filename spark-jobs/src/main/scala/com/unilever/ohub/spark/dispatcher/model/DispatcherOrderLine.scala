package com.unilever.ohub.spark.dispatcher
package model

import com.unilever.ohub.spark.domain.entity.OrderLine

object DispatcherOrderLine {
  def fromOrderLine(orderLine: OrderLine): DispatcherOrderLine = {
    DispatcherOrderLine(
      AMOUNT = orderLine.amount,
      CAMPAIGN_LABEL = orderLine.campaignLabel,
      COMMENTS = orderLine.comment,
      ODL_INTEGRATION_ID = orderLine.concatId,
      COUNTRY_CODE = orderLine.countryCode,
      UNIT_PRICE_CURRENCY = orderLine.currency,
      DELETE_FLAG = orderLine.isActive.invert,
      LOYALTY_POINTS = orderLine.loyaltyPoints,
      ODS_CREATED = orderLine.ohubCreated.mapWithDefaultPattern,
      ODS_UPDATED = orderLine.ohubUpdated.mapWithDefaultPattern,
      ORD_INTEGRATION_ID = orderLine.orderConcatId,
      UNIT_PRICE = orderLine.pricePerUnit,
      PRD_INTEGRATION_ID = orderLine.productConcatId,
      QUANTITY = orderLine.quantityOfUnits,
      SOURCE = orderLine.sourceName
    )
  }
}

case class DispatcherOrderLine(
    AMOUNT: BigDecimal,
    CAMPAIGN_LABEL: Option[String],
    COMMENTS: Option[String],
    ODL_INTEGRATION_ID: String,
    COUNTRY_CODE: String,
    UNIT_PRICE_CURRENCY: Option[String],
    DELETE_FLAG: Boolean,
    LOYALTY_POINTS: Option[BigDecimal],
    ODS_CREATED: String,
    ODS_UPDATED: String,
    ORD_INTEGRATION_ID: String,
    UNIT_PRICE: Option[BigDecimal],
    PRD_INTEGRATION_ID: String,
    QUANTITY: Long,
    SOURCE: String
)
