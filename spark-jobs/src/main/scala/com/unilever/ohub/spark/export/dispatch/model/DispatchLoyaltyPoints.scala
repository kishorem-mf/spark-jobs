package com.unilever.ohub.spark.export.dispatch.model

import com.unilever.ohub.spark.export.DispatcherOutboundEntity

case class DispatchLoyaltyPoints(
    CP_ORIG_INTEGRATION_ID: String,
    COUNTRY_CODE: String,
    CP_LNKD_INTEGRATION_ID: String,
    EMAIL_ADDRESS: String = "",
    EARNED: String,
    SPENT: String,
    ACTUAL: String,
    GOAL: String,
    UPDATED_AT: String
) extends DispatcherOutboundEntity
