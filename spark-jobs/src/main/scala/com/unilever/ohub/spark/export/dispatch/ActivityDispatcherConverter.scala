package com.unilever.ohub.spark.export.dispatch

import com.unilever.ohub.spark.domain.entity.Activity
import com.unilever.ohub.spark.export.dispatch.model.DispatchActivity
import com.unilever.ohub.spark.export.{Converter, InvertedBooleanToYNConverter, TypeConversionFunctions}

object ActivityDispatcherConverter extends Converter[Activity, DispatchActivity] with TypeConversionFunctions with DispatchTransformationFunctions {
  override def convert(implicit activity: Activity, explain: Boolean = false): DispatchActivity = {
    DispatchActivity(
      CP_ORIG_INTEGRATION_ID = getValue("contactPersonConcatId"),
      RAC_INTEGRATION_ID = getValue("concatId"),
      SOURCE = getValue("sourceName"),
      COUNTRY_CODE = getValue("countryCode"),
      CREATED_AT = getValue("ohubCreated"),
      UPDATED_AT = getValue("ohubUpdated"),
      DELETE_FLAG = getValue("isActive", Some(InvertedBooleanToYNConverter)),
      CONTACT_DATE = getValue("activityDate"),
      ACTION_TYPE = getValue("actionType"),
      ACTIVITY_NAME = getValue("name"),
      ACTIVITY_DETAILS = getValue("details")
    )
  }
}
