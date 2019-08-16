package com.unilever.ohub.spark.export.acm

import com.unilever.ohub.spark.domain.entity.OrderLine
import com.unilever.ohub.spark.export.acm.model.AcmOrderLine
import com.unilever.ohub.spark.export.{Converter, InvertedBooleanToYNConverter, TypeConversionFunctions}

object OrderLineAcmConverter extends Converter[OrderLine, AcmOrderLine] with TypeConversionFunctions with AcmTransformationFunctions {

  override def convert(orderLine: OrderLine): AcmOrderLine = {
    implicit val ol = orderLine

    AcmOrderLine(
      ORDERLINE_ID = getValue("concatId"),
      ORD_INTEGRATION_ID = getValue("orderConcatId"),
      QUANTITY = getValue("quantityOfUnits"),
      AMOUNT = getValue("amount"),
      LOYALTY_POINTS = getValue("loyaltyPoints"),
      PRD_INTEGRATION_ID = getValue("productOhubId"),
      CAMPAIGN_LABEL = getValue("campaignLabel"),
      COMMENTS = getValue("comment"),
      DELETED_FLAG = getValue("isActive", Some(InvertedBooleanToYNConverter))
    )
  }

}
