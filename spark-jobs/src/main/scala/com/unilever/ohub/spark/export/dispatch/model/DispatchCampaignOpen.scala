package com.unilever.ohub.spark.export.dispatch.model

import com.unilever.ohub.spark.export.DispatcherOutboundEntity

case class DispatchCampaignOpen(
    TRACKING_ID: String,
    CAMPAIGN_WAVE_RESPONSE_ID: String,
    COUNTRY_CODE: String,
    OPEN_DATE: String,
    CREATED_AT: String,
    UPDATED_AT: String
) extends DispatcherOutboundEntity

