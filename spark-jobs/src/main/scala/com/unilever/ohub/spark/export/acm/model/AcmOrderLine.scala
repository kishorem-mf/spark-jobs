package com.unilever.ohub.spark.export.acm.model

import com.unilever.ohub.spark.export.ACMOutboundEntity

case class AcmOrderLine(
    ORDERLINE_ID: String,
    ORD_INTEGRATION_ID: String,
    QUANTITY: String,
    AMOUNT: String,
    LOYALTY_POINTS: String,
    PRD_INTEGRATION_ID: String,
    SAMPLE_ID: String = "",
    CAMPAIGN_LABEL: String,
    COMMENTS: String,
    DELETED_FLAG: String
) extends ACMOutboundEntity
