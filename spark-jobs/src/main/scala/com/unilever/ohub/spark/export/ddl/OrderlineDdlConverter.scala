package com.unilever.ohub.spark.export.ddl

import com.unilever.ohub.spark.domain.entity.OrderLine
import com.unilever.ohub.spark.export._
import com.unilever.ohub.spark.export.ddl.model.DdlOrderline

object OrderlineDdlConverter extends Converter[OrderLine, DdlOrderline] with TypeConversionFunctions {
  override def convert(implicit order: OrderLine, explain: Boolean = false): DdlOrderline = {
    DdlOrderline(
    Id = getValue("sourceEntityId"),
    Quantity = getValue("quantityOfUnits"),
    `List Price` = getValue("pricePerUnit"),
    `Sales Price` = Option.empty,
    `Product Id` = getValue("productSourceEntityId"),
    OpportunityId = getValue("orderConcatId", getSourceEntityIdBasedOnConcatId),
    FOC = Option.empty,
    `Unit Of Measure` = Option.empty,
    `Total Price` = getValue("amount"),
    Discount = Option.empty,
    `Discount%` = Option.empty
    )
  }

}
