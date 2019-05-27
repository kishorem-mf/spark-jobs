package com.unilever.ohub.spark.export.dispatch.model

import com.unilever.ohub.spark.export.OutboundEntity

case class DispatchCampaignOpen(
    TRACKING_ID: String,
    CAMPAIGN_WAVE_RESPONSE_ID: String,
    COUNTRY_CODE: String,
    OPEN_DATE: String
) extends OutboundEntity

