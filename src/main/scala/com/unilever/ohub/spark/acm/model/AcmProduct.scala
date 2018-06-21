package com.unilever.ohub.spark.acm.model

case class AcmProduct(
    // Deliberate misspelling as the consuming system requires it :'(
    COUNTRY_CODE: Option[String],
    PRODUCT_NAME: Option[String],
    PRD_INTEGRATION_ID: String,
    EAN_CODE: Option[String],
    MRDR_CODE: Option[String],
    CREATED_AT: Option[String],
    UPDATED_AT: Option[String],
    DELETE_FLAG: Option[String]
)
