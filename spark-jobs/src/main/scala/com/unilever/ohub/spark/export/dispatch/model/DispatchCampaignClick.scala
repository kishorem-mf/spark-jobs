package com.unilever.ohub.spark.export.dispatch.model

import com.unilever.ohub.spark.export.OutboundEntity

case class DispatchCampaignClick(
    TRACKING_ID: String,
    CAMPAIGN_WAVE_RESPONSE_ID: String,
    COUNTRY_CODE: String,
    CLICK_DATE: String,
    CLICKED_URL: String,
    MOBILE_DEVICE: String,
    OPERATING_SYSTEM: String,
    BROWSER_NAME: String,
    BROWSER_VERSION: String
) extends OutboundEntity

