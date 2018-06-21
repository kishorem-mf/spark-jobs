package com.unilever.ohub.spark.acm.model

case class AcmOrderLine(
    ORDERLINE_ID: String,
    ORD_INTEGRATION_ID: String,
    QUANTITY: Long,
    AMOUNT: BigDecimal,
    LOYALTY_POINTS: Option[BigDecimal],
    PRD_INTEGRATION_ID: String,
    SAMPLE_ID: String,
    CAMPAIGN_LABEL: Option[String],
    COMMENTS: Option[String],
    DELETED_FLAG: String
)
