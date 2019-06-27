package com.unilever.ohub.spark.export.acm

import com.unilever.ohub.spark.domain.entity.Activity
import com.unilever.ohub.spark.export.acm.model.AcmActivity
import com.unilever.ohub.spark.export.{Converter, TransformationFunctions}

object ActivityAcmConverter extends Converter[Activity, AcmActivity] with TransformationFunctions with AcmTransformationFunctions {

  override def convert(activity: Activity): AcmActivity = {
    AcmActivity(
      ACTIVITY_ID = activity.concatId,
      COUNTRY_CODE = activity.countryCode,
      CP_ORIG_INTEGRATION_ID = activity.contactPersonOhubId,
      DELETE_FLAG = booleanToYNConverter(!activity.isActive),
      DATE_CREATED = activity.dateCreated,
      DATE_UPDATED = activity.dateUpdated,
      DETAILS = activity.details,
      TYPE = activity.actionType,
      NAME = activity.name
    )
  }
}
