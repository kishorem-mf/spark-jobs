package com.unilever.ohub.spark.export.dispatch.model

import com.unilever.ohub.spark.export.DispatcherOutboundEntity

case class DispatchCampaignBounce(
    CAMPAIGN_WAVE_RESPONSE_ID: String,
    COUNTRY_CODE: String,
    BOUNCE_DATE: String,
    FAILURE_TYPE: String,
    FAILURE_REASON: String,
    CREATED_AT: String,
    UPDATED_AT: String
) extends DispatcherOutboundEntity
