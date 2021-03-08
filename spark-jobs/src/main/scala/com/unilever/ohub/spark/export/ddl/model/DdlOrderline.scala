package com.unilever.ohub.spark.export.ddl.model

import com.unilever.ohub.spark.export.DDLOutboundEntity

case class DdlOrderline(
                         Id: String,
                         Quantity: String,
                         `List Price`: String,
                         `Sales Price`: String,
                         `Product Id`: String,
                         OpportunityId: String,
                         FOC: String,
                         `Unit Of Measure`: String,
                         `Total Price`: String,
                         Discount: String,
                         `Discount%`: String
                       ) extends DDLOutboundEntity
