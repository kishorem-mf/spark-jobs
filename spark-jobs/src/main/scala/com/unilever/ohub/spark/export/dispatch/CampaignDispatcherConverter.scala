package com.unilever.ohub.spark.export.dispatch

import com.unilever.ohub.spark.domain.entity.Campaign
import com.unilever.ohub.spark.export.dispatch.model.DispatchCampaign
import com.unilever.ohub.spark.export.{Converter, TypeConversionFunctions}

object CampaignDispatcherConverter extends Converter[Campaign, DispatchCampaign] with TypeConversionFunctions with DispatchTransformationFunctions {
  override def convert(implicit campaign: Campaign, explain: Boolean = false): DispatchCampaign = {
    DispatchCampaign(
      CP_ORIG_INTEGRATION_ID = getValue("contactPersonConcatId"),
      COUNTRY_CODE = getValue("countryCode"),
      CAMPAIGN_NAME = getValue("campaignName"),
      CAMPAIGN_SPECIFICATION = getValue("campaignSpecification"),
      CAMPAIGN_WAVE_START_DATE = getValue("campaignWaveStartDate"),
      CAMPAIGN_WAVE_END_DATE = getValue("campaignWaveEndDate"),
      CAMPAIGN_WAVE_STATUS = getValue("campaignWaveStatus"),
      CONCAT_ID = getValue("concatId"),
      CAMPAIGN_ID = getValue("campaignId"),
      DELIVERY_ID = getValue("deliveryId"),
      CAMPAIGN_CONCAT_ID = getValue("campaignConcatId"),
      CREATED_AT = getValue("ohubCreated"),
      UPDATED_AT = getValue("ohubUpdated")
    )
  }
}
