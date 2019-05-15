package com.unilever.ohub.spark.export.dispatch.model

import com.unilever.ohub.spark.export.OutboundEntity

case class DispatchActivity(
    CP_ORIG_INTEGRATION_ID: String,
    RAC_INTEGRATION_ID: String,
    SOURCE: String,
    COUNTRY_CODE: String,
    CREATED_AT: String,
    UPDATED_AT: String,
    DELETE_FLAG: String,
    CONTACT_DATE: String,
    CAMPAIGN_NAME: String = "",
    CAMPAIGN_DRIVER: String = "",
    PROGRAM: String = "",
    RESULT: String = "",
    ACTION_TYPE: String,
    ACTIVITY_NAME: String,
    ACTIVITY_DETAILS: String,
    ACTIVITY_RESULT: String = ""
) extends OutboundEntity
