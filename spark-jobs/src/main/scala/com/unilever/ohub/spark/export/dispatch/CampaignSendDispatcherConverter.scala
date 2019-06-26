package com.unilever.ohub.spark.export.dispatch

import com.unilever.ohub.spark.domain.entity.CampaignSend
import com.unilever.ohub.spark.export.dispatch.model.DispatchCampaignSend
import com.unilever.ohub.spark.export.{Converter, TransformationFunctions}

object CampaignSendDispatcherConverter extends Converter[CampaignSend, DispatchCampaignSend] with TransformationFunctions with DispatchTransformationFunctions {
  override def convert(send: CampaignSend): DispatchCampaignSend =
    DispatchCampaignSend(
      CP_ORIG_INTEGRATION_ID = send.contactPersonConcatId,
      CWS_INTEGRATION_ID = send.concatId,
      CWS_DATE = send.sendDate,
      COUNTRY_CODE = send.countryCode,
      CAMPAIGN_NAME = send.campaignName,
      WAVE_NAME = send.waveName,
      CHANNEL = send.communicationChannel,
      CAMPAIGN_WAVE_RESPONSE_ID = send.deliveryLogId,
      CONTROL_POPULATION = send.isControlGroupMember,
      PROOF_GROUP_MEMBER = send.isProofGroupMember,
      SELECTION_FOR_OFFLINE_CHANNELS = send.selectionForOfflineChannels,
      CAMPAIGN_ID = send.campaignId,
      DELIVERY_ID = send.deliveryId,
      CAMPAIGN_CONCAT_ID = send.campaignConcatId,
      CREATED_AT = send.ohubCreated,
      UPDATED_AT = send.ohubUpdated
    )
}
