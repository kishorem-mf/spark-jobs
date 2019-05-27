package com.unilever.ohub.spark.export.dispatch

import com.unilever.ohub.spark.domain.entity.CampaignClick
import com.unilever.ohub.spark.export.dispatch.model.DispatchCampaignClick
import com.unilever.ohub.spark.export.{Converter, TransformationFunctions}

object CampaignClickDispatcherConverter extends Converter[CampaignClick, DispatchCampaignClick] with TransformationFunctions with DispatchTransformationFunctions {

  override def convert(click: CampaignClick): DispatchCampaignClick = {
    DispatchCampaignClick(
      TRACKING_ID = click.trackingId,
      CAMPAIGN_WAVE_RESPONSE_ID = click.deliveryLogId,
      COUNTRY_CODE = click.countryCode,
      CLICK_DATE = click.clickDate,
      CLICKED_URL = click.clickedUrl,
      MOBILE_DEVICE = click.isOnMobileDevice,
      OPERATING_SYSTEM = click.operatingSystem,
      BROWSER_NAME = click.browserName,
      BROWSER_VERSION = click.browserVersion
    )
  }
}
