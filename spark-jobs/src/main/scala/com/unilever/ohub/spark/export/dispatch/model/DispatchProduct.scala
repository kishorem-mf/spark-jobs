package com.unilever.ohub.spark.export.dispatch.model

import com.unilever.ohub.spark.export.OutboundEntity

case class DispatchProduct(
    COUNTRY_CODE: String,
    SOURCE: String,
    CREATED_AT: String,
    UPDATED_AT: String,
    PRODUCT_NAME: String,
    EAN_CODE: String,
    DELETE_FLAG: String,
    MRDR_CODE: String,
    PRD_INTEGRATION_ID: String,
    EAN_CODE_DISPATCH_UNIT: String,
    CATEGORY: String,
    SUB_CATEGORY: String,
    BRAND: String,
    SUB_BRAND: String,
    ITEM_TYPE: String,
    UNIT: String,
    UNIT_PRICE_CURRENCY: String,
    UNIT_PRICE: String
) extends OutboundEntity
