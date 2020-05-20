package com.unilever.ohub.spark.export.dispatch.model

import com.unilever.ohub.spark.export.DispatcherOutboundEntity

case class DispatchLoyaltyPoints(
    LOY_ORIG_INTEGRATION_ID: String,
    CP_ORIG_INTEGRATION_ID: String,
    CP_LNKD_INTEGRATION_ID: String,
    COUNTRY_CODE: String,
    DELETE_FLAG: String,
    GOLDEN_RECORD_FLAG: String,
    CREATED_AT: String,
    UPDATED_AT: String,
    OPR_ORIG_INTEGRATION_ID: String,
    OPR_LNKD_INTEGRATION_ID: String,
    EMAIL_ADDRESS: String = "",
    EARNED: String,
    SPENT: String,
    ACTUAL: String,
    GOAL: String,
    NAME: String,
    IMAGE_URL: String,
    LANDING_PAGE_URL: String,
    EAN_CODE: String
) extends DispatcherOutboundEntity
