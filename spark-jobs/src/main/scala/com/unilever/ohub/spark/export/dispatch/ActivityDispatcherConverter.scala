package com.unilever.ohub.spark.export.dispatch

import com.unilever.ohub.spark.domain.entity.Activity
import com.unilever.ohub.spark.export.dispatch.model.DispatchActivity
import com.unilever.ohub.spark.export.{Converter, TransformationFunctions}

object ActivityDispatcherConverter extends Converter[Activity, DispatchActivity] with TransformationFunctions with DispatchTransformationFunctions {
  override def convert(activity: Activity): DispatchActivity =
    DispatchActivity(
      CP_ORIG_INTEGRATION_ID = activity.contactPersonConcatId,
      RAC_INTEGRATION_ID = activity.concatId,
      SOURCE = activity.sourceName,
      COUNTRY_CODE = activity.countryCode,
      CREATED_AT = activity.ohubCreated,
      UPDATED_AT = activity.ohubUpdated,
      DELETE_FLAG = booleanToYNConverter(!activity.isActive),
      CONTACT_DATE = activity.activityDate,
      ACTION_TYPE = activity.actionType,
      ACTIVITY_NAME = activity.name,
      ACTIVITY_DETAILS = activity.details
    )
}
