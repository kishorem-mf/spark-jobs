package com.unilever.ohub.spark.export.dispatch

import com.unilever.ohub.spark.domain.entity.CampaignBounce
import com.unilever.ohub.spark.export.dispatch.model.DispatchCampaignBounce
import com.unilever.ohub.spark.export.{Converter, TypeConversionFunctions}

object CampaignBounceDispatcherConverter extends Converter[CampaignBounce, DispatchCampaignBounce] with TypeConversionFunctions with DispatchTypeConversionFunctions {

  override def convert(implicit bounce: CampaignBounce, explain: Boolean = false): DispatchCampaignBounce = {
    DispatchCampaignBounce(
      CAMPAIGN_WAVE_RESPONSE_ID = getValue("deliveryLogId"),
      COUNTRY_CODE = getValue("countryCode"),
      BOUNCE_DATE = getValue("bounceDate"),
      FAILURE_TYPE = getValue("failureType"),
      FAILURE_REASON = getValue("failureReason"),
      CREATED_AT = getValue("ohubCreated"),
      UPDATED_AT = getValue("ohubUpdated")
    )
  }
}
