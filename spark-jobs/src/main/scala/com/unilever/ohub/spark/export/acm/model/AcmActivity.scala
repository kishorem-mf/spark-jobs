package com.unilever.ohub.spark.export.acm.model

import com.unilever.ohub.spark.export.ACMOutboundEntity

case class AcmActivity(
    ACTIVITY_ID: String,
    COUNTRY_CODE: String,
    CP_ORIG_INTEGRATION_ID: String,
    DELETE_FLAG: String,
    DATE_CREATED: String,
    DATE_UPDATED: String,
    DETAILS: String,
    SUBSCRIBED: String = "",
    TYPE: String,
    NAME: String) extends ACMOutboundEntity
