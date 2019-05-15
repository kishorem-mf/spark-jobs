package com.unilever.ohub.spark.export.dispatch

import com.unilever.ohub.spark.domain.entity.CampaignBounce
import com.unilever.ohub.spark.export.dispatch.model.DispatchCampaignBounce
import com.unilever.ohub.spark.export.{Converter, TransformationFunctions}

object CampaignBounceDispatcherConverter extends Converter[CampaignBounce, DispatchCampaignBounce] with TransformationFunctions with DispatchTransformationFunctions {

  override def convert(bounce: CampaignBounce): DispatchCampaignBounce = {
    DispatchCampaignBounce(
      CAMPAIGN_WAVE_RESPONSE_ID = bounce.deliveryLogId,
      COUNTRY_CODE = bounce.countryCode,
      BOUNCE_DATE = bounce.bounceDate,
      FAILURE_TYPE = bounce.failureType,
      FAILURE_REASON = bounce.failureReason
    )
  }
}
