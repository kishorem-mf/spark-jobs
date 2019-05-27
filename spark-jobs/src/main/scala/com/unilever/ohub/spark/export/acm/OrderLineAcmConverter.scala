package com.unilever.ohub.spark.export.acm

import com.unilever.ohub.spark.domain.entity.OrderLine
import com.unilever.ohub.spark.export.acm.model.AcmOrderLine
import com.unilever.ohub.spark.export.{Converter, TransformationFunctions}

object OrderLineAcmConverter extends Converter[OrderLine, AcmOrderLine] with TransformationFunctions with AcmTransformationFunctions {

  override def convert(orderLine: OrderLine): AcmOrderLine = {
    AcmOrderLine(
      ORDERLINE_ID = orderLine.concatId,
      ORD_INTEGRATION_ID = orderLine.orderConcatId,
      QUANTITY = orderLine.quantityOfUnits.toString,
      AMOUNT = orderLine.amount,
      LOYALTY_POINTS = orderLine.loyaltyPoints,
      PRD_INTEGRATION_ID = orderLine.productConcatId,
      CAMPAIGN_LABEL = orderLine.campaignLabel,
      COMMENTS = orderLine.comment,
      DELETED_FLAG = !orderLine.isActive
    )
  }

}
