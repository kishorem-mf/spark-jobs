package com.unilever.ohub.spark.export.dispatch

import com.unilever.ohub.spark.domain.entity.OrderLine
import com.unilever.ohub.spark.export.dispatch.model.DispatchOrderLine
import com.unilever.ohub.spark.export.{Converter, InvertedBooleanToYNConverter, TypeConversionFunctions}

object OrderLineDispatchConverter extends Converter[OrderLine, DispatchOrderLine] with TypeConversionFunctions with DispatchTypeConversionFunctions {

  override def convert(implicit orderline: OrderLine, explain: Boolean = false): DispatchOrderLine = {
    DispatchOrderLine(
      COUNTRY_CODE = getValue("countryCode"),
      SOURCE = getValue("sourceName"),
      DELETE_FLAG = getValue("isActive", InvertedBooleanToYNConverter),
      ORD_INTEGRATION_ID = getValue("orderConcatId"),
      ODL_INTEGRATION_ID = getValue("concatId"),
      PRD_INTEGRATION_ID = getValue("productConcatId"),
      CAMPAIGN_LABEL = getValue("campaignLabel"),
      COMMENTS = getValue("comment"),
      QUANTITY = getValue("quantityOfUnits"),
      AMOUNT = getValue("amount"),
      LOYALTY_POINTS = getValue("loyaltyPoints"),
      UNIT_PRICE = getValue("pricePerUnit"),
      UNIT_PRICE_CURRENCY = getValue("currency"),
      ODS_CREATED = getValue("ohubCreated"),
      ODS_UPDATED = getValue("ohubUpdated")
    )
  }
}
