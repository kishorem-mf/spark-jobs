package com.unilever.ohub.spark.export.dispatch.model

import com.unilever.ohub.spark.export.OutboundEntity

case class DispatchCampaign(
    CP_ORIG_INTEGRATION_ID: String,
    COUNTRY_CODE: String,
    CAMPAIGN_NAME: String,
    CAMPAIGN_SPECIFICATION: String,
    CAMPAIGN_WAVE_START_DATE: String,
    CAMPAIGN_WAVE_END_DATE: String,
    CAMPAIGN_WAVE_STATUS: String,
    CONCAT_ID: String,
    CAMPAIGN_ID: String,
    DELIVERY_ID: String,
    CAMPAIGN_CONCAT_ID: String
) extends OutboundEntity
