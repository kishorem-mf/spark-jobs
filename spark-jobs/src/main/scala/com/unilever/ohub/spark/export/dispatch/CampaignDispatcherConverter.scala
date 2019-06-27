package com.unilever.ohub.spark.export.dispatch

import com.unilever.ohub.spark.domain.entity.Campaign
import com.unilever.ohub.spark.export.dispatch.model.DispatchCampaign
import com.unilever.ohub.spark.export.{Converter, TransformationFunctions}

object CampaignDispatcherConverter extends Converter[Campaign, DispatchCampaign] with TransformationFunctions with DispatchTransformationFunctions {
  override def convert(campaign: Campaign): DispatchCampaign =
    DispatchCampaign(
      CP_ORIG_INTEGRATION_ID = campaign.contactPersonConcatId,
      COUNTRY_CODE = campaign.countryCode,
      CAMPAIGN_NAME = campaign.campaignName,
      CAMPAIGN_SPECIFICATION = campaign.campaignSpecification,
      CAMPAIGN_WAVE_START_DATE = campaign.campaignWaveStartDate,
      CAMPAIGN_WAVE_END_DATE = campaign.campaignWaveEndDate,
      CAMPAIGN_WAVE_STATUS = campaign.campaignWaveStatus,
      CONCAT_ID = campaign.concatId,
      CAMPAIGN_ID = campaign.campaignId,
      DELIVERY_ID = campaign.deliveryId,
      CAMPAIGN_CONCAT_ID = campaign.campaignConcatId,
      CREATED_AT = campaign.ohubCreated,
      UPDATED_AT = campaign.ohubUpdated
    )
}
