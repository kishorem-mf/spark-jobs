package com.unilever.ohub.spark.export.dispatch

import com.unilever.ohub.spark.domain.entity.CampaignSend
import com.unilever.ohub.spark.export.dispatch.model.DispatchCampaignSend
import com.unilever.ohub.spark.export.{BooleanTo10Converter, Converter, TypeConversionFunctions}

object CampaignSendDispatcherConverter extends Converter[CampaignSend, DispatchCampaignSend] with TypeConversionFunctions with DispatchTransformationFunctions {
  override def convert(implicit send: CampaignSend, explain: Boolean = false): DispatchCampaignSend = {
    DispatchCampaignSend(
      CP_ORIG_INTEGRATION_ID = getValue("contactPersonConcatId"),
      CWS_INTEGRATION_ID = getValue("concatId"),
      CWS_DATE = getValue("sendDate"),
      COUNTRY_CODE = getValue("countryCode"),
      CAMPAIGN_NAME = getValue("campaignName"),
      WAVE_NAME = getValue("waveName"),
      CHANNEL = getValue("communicationChannel"),
      CAMPAIGN_WAVE_RESPONSE_ID = getValue("deliveryLogId"),
      CONTROL_POPULATION = getValue("isControlGroupMember", Some(BooleanTo10Converter)),
      PROOF_GROUP_MEMBER = getValue("isProofGroupMember", Some(BooleanTo10Converter)),
      SELECTION_FOR_OFFLINE_CHANNELS = getValue("selectionForOfflineChannels"),
      CAMPAIGN_ID = getValue("campaignId"),
      DELIVERY_ID = getValue("deliveryId"),
      CAMPAIGN_CONCAT_ID = getValue("campaignConcatId"),
      CREATED_AT = getValue("ohubCreated"),
      UPDATED_AT = getValue("ohubUpdated")
    )
  }
}
