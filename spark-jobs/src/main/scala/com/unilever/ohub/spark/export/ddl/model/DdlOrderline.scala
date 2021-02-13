package com.unilever.ohub.spark.export.ddl.model

import com.unilever.ohub.spark.export.DDLOutboundEntity

case class DdlOrderline(
                         id: String,
                         quantity: String,
                         listPrice: String,
                         salesPrice: String,
                         productId: String,
                         opportunityId: String,
                         foc: String,
                         unitOfMeasure: String,
                         totalPrice: String,
                         discount: String,
                         discountPercentage: String
                       ) extends DDLOutboundEntity
