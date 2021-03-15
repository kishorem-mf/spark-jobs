package com.unilever.ohub.spark.export.ddl.model

import com.unilever.ohub.spark.export.DDLOutboundEntity

case class DdlOrder(
                     createdDate: String,
                     accountId: String,
                     createdBySAP: String,
                     purchaseOrderType: String,
                     niv: String,
                     totalGrossPrice: String,
                     totalSurcharge: String,
                     discount: String,
                     rejectionStatus: String,
                     deliveryStatus: String,
                     amount: String,
                     orderName: String
                   ) extends DDLOutboundEntity
