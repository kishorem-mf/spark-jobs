package com.unilever.ohub.spark.export.dispatch.model

import com.unilever.ohub.spark.export.DispatcherOutboundEntity

case class DispatchCampaignClick(
    TRACKING_ID: String,
    CAMPAIGN_WAVE_RESPONSE_ID: String,
    COUNTRY_CODE: String,
    CLICK_DATE: String,
    CLICKED_URL: String,
    MOBILE_DEVICE: String,
    OPERATING_SYSTEM: String,
    BROWSER_NAME: String,
    BROWSER_VERSION: String,
    CREATED_AT: String,
    UPDATED_AT: String
) extends DispatcherOutboundEntity

