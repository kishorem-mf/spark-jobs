package com.unilever.ohub.spark.export.dispatch

import com.unilever.ohub.spark.domain.entity.CampaignClick
import com.unilever.ohub.spark.export.dispatch.model.DispatchCampaignClick
import com.unilever.ohub.spark.export.{Converter, TypeConversionFunctions}

object CampaignClickDispatcherConverter extends Converter[CampaignClick, DispatchCampaignClick] with TypeConversionFunctions with DispatchTransformationFunctions {

  override def convert(implicit click: CampaignClick, explain: Boolean = false): DispatchCampaignClick = {
    DispatchCampaignClick(
      TRACKING_ID = getValue("trackingId"),
      CAMPAIGN_WAVE_RESPONSE_ID = getValue("deliveryLogId"),
      COUNTRY_CODE = getValue("countryCode"),
      CLICK_DATE = getValue("clickDate"),
      CLICKED_URL = getValue("clickedUrl"),
      MOBILE_DEVICE = getValue("isOnMobileDevice"),
      OPERATING_SYSTEM = getValue("operatingSystem"),
      BROWSER_NAME = getValue("browserName"),
      BROWSER_VERSION = getValue("browserVersion"),
      CREATED_AT = getValue("ohubCreated"),
      UPDATED_AT = getValue("ohubUpdated")
    )
  }
}
