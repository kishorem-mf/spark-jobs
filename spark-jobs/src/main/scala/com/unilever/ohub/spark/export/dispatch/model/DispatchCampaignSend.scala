package com.unilever.ohub.spark.export.dispatch.model

import com.unilever.ohub.spark.export.OutboundEntity

case class DispatchCampaignSend(
    CP_ORIG_INTEGRATION_ID: String,
    CWS_INTEGRATION_ID: String,
    CWS_DATE: String,
    COUNTRY_CODE: String,
    CAMPAIGN_NAME: String,
    WAVE_NAME: String,
    CHANNEL: String,
    CAMPAIGN_WAVE_RESPONSE_ID: String,
    CONTROL_POPULATION: String,
    PROOF_GROUP_MEMBER: String,
    SELECTION_FOR_OFFLINE_CHANNELS: String,
    CAMPAIGN_SPECIFICATION: String = "",
    CAMPAIGN_ID: String,
    DELIVERY_ID: String,
    CAMPAIGN_CONCAT_ID: String
) extends OutboundEntity

