package com.unilever.ohub.spark.export.dispatch

import com.unilever.ohub.spark.domain.entity.OrderLine
import com.unilever.ohub.spark.export.dispatch.model.DispatchOrderLine
import com.unilever.ohub.spark.export.{Converter, TransformationFunctions}

object OrderLineDispatchConverter extends Converter[OrderLine, DispatchOrderLine] with TransformationFunctions with DispatchTransformationFunctions {

  override def convert(orderline: OrderLine): DispatchOrderLine = {
    DispatchOrderLine(
      COUNTRY_CODE = orderline.countryCode,
      SOURCE = orderline.sourceName,
      DELETE_FLAG = booleanToYNConverter(!orderline.isActive),
      ORD_INTEGRATION_ID = orderline.orderConcatId,
      ODL_INTEGRATION_ID = orderline.concatId,
      PRD_INTEGRATION_ID = orderline.productConcatId,
      CAMPAIGN_LABEL = orderline.campaignLabel,
      COMMENTS = orderline.comment,
      QUANTITY = orderline.quantityOfUnits.toString,
      AMOUNT = orderline.amount,
      LOYALTY_POINTS = orderline.loyaltyPoints,
      UNIT_PRICE = orderline.pricePerUnit,
      UNIT_PRICE_CURRENCY = orderline.currency,
      ODS_CREATED = orderline.ohubCreated,
      ODS_UPDATED = orderline.ohubUpdated
    )
  }
}
