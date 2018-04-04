package com.unilever.ohub.spark.data.ufs

case class UFSOrderLine(
    ORDER_ID: String,
    ORDERLINE_ID: String,
    PRD_INTEGRATION_ID: String,
    QUANTITY: Option[Long],
    AMOUNT: Option[Double],
    SAMPLE_ID: String
)
