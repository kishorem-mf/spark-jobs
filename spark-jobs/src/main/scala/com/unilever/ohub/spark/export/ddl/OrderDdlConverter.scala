package com.unilever.ohub.spark.export.ddl

import com.unilever.ohub.spark.domain.entity.Order
import com.unilever.ohub.spark.export._
import com.unilever.ohub.spark.export.ddl.model.DdlOrder

object OrderDdlConverter extends Converter[Order, DdlOrder] with TypeConversionFunctions {
  override def convert(implicit order: Order, explain: Boolean = false): DdlOrder = {


    DdlOrder(
      createdDate = getValue("dateCreated"),
      accountId = getValue("operatorConcatId", getSourceEntityIdBasedOnConcatId),
      createdBySAP = Option.empty,
      purchaseOrderType = Option.empty,
      niv = Option.empty,
      totalGrossPrice = Option.empty,
      totalSurcharge = Option.empty,
      discount = Option.empty,
      rejectionStatus = Option.empty,
      deliveryStatus = Option.empty,
      amount = getValue("amount"),
      orderName = getValue("comment")
    )
  }

}
