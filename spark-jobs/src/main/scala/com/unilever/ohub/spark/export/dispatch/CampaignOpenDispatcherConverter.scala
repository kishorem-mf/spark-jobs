package com.unilever.ohub.spark.export.dispatch

import com.unilever.ohub.spark.domain.entity.CampaignOpen
import com.unilever.ohub.spark.export.dispatch.model.DispatchCampaignOpen
import com.unilever.ohub.spark.export.{Converter, TypeConversionFunctions}

object CampaignOpenDispatcherConverter extends Converter[CampaignOpen, DispatchCampaignOpen] with TypeConversionFunctions with DispatchTransformationFunctions {

  override def convert(open: CampaignOpen): DispatchCampaignOpen = {
    DispatchCampaignOpen(
      TRACKING_ID = open.trackingId,
      CAMPAIGN_WAVE_RESPONSE_ID = open.deliveryLogId,
      COUNTRY_CODE = open.countryCode,
      OPEN_DATE = open.openDate,
      CREATED_AT = open.ohubCreated,
      UPDATED_AT = open.ohubUpdated
    )
  }
}
