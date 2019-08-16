package com.unilever.ohub.spark.export.acm

import com.unilever.ohub.spark.domain.entity.Activity
import com.unilever.ohub.spark.export.acm.model.AcmActivity
import com.unilever.ohub.spark.export.{Converter, InvertedBooleanToYNConverter}

object ActivityAcmConverter extends Converter[Activity, AcmActivity] with AcmTransformationFunctions {

  override def convert(activity: Activity): AcmActivity = {
    implicit val act: Activity = activity
    implicit val explain: Boolean = false

    AcmActivity(
      ACTIVITY_ID = getValue("concatId"),
      COUNTRY_CODE = getValue("countryCode"),
      CP_ORIG_INTEGRATION_ID = getValue("contactPersonOhubId"),
      DELETE_FLAG = getValue("isActive", Some(InvertedBooleanToYNConverter)),
      DATE_CREATED = getValue("dateCreated"),
      DATE_UPDATED = getValue("dateUpdated"),
      DETAILS = getValue("details"),
      TYPE = getValue("actionType"),
      NAME = getValue("name")
    )
  }
}
