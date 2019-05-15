package com.unilever.ohub.spark.export.dispatch.model

import com.unilever.ohub.spark.export.OutboundEntity

case class DispatchOrderLine(
    COUNTRY_CODE: String,
    SOURCE: String,
    DELETE_FLAG: String,
    ORD_INTEGRATION_ID: String,
    ODL_INTEGRATION_ID: String,
    PRD_INTEGRATION_ID: String,
    CAMPAIGN_LABEL: String,
    COMMENTS: String,
    QUANTITY: String,
    AMOUNT: String,
    LOYALTY_POINTS: String,
    UNIT_PRICE: String,
    UNIT_PRICE_CURRENCY: String,
    ODS_CREATED: String,
    ODS_UPDATED: String
) extends OutboundEntity
