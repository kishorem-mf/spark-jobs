package com.unilever.ohub.spark.export.ddl

import com.unilever.ohub.spark.domain.entity.OrderLine
import com.unilever.ohub.spark.export._
import com.unilever.ohub.spark.export.ddl.model.DdlOrderline

object OrderlineDdlConverter extends Converter[OrderLine, DdlOrderline] with TypeConversionFunctions {
  override def convert(implicit order: OrderLine, explain: Boolean = false): DdlOrderline = {
    DdlOrderline(
    id = getValue("sourceEntityId"),
    quantity = getValue("quantityOfUnits"),
    listPrice = getValue("pricePerUnit"),
    salesPrice = Option.empty,
    productId = getValue("productSourceEntityId"),
    opportunityId = getValue("orderConcatId", getSourceEntityIdBasedOnConcatId),
    foc = Option.empty,
    unitOfMeasure = Option.empty,
    totalPrice = getValue("amount"),
    discount = Option.empty,
    discountPercentage = Option.empty
    )
  }

}
