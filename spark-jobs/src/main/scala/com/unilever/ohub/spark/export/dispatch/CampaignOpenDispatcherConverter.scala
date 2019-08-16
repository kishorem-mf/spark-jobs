package com.unilever.ohub.spark.export.dispatch

import com.unilever.ohub.spark.domain.entity.CampaignOpen
import com.unilever.ohub.spark.export.dispatch.model.DispatchCampaignOpen
import com.unilever.ohub.spark.export.{Converter, TypeConversionFunctions}

object CampaignOpenDispatcherConverter extends Converter[CampaignOpen, DispatchCampaignOpen] with TypeConversionFunctions with DispatchTransformationFunctions {

  override def convert(implicit open: CampaignOpen, explain: Boolean = false): DispatchCampaignOpen = {
    DispatchCampaignOpen(
      TRACKING_ID = getValue("trackingId"),
      CAMPAIGN_WAVE_RESPONSE_ID = getValue("deliveryLogId"),
      COUNTRY_CODE = getValue("countryCode"),
      OPEN_DATE = getValue("openDate"),
      CREATED_AT = getValue("ohubCreated"),
      UPDATED_AT = getValue("ohubUpdated")
    )
  }
}
