package com.unilever.ohub.spark.export.acm

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.DomainEntityUtils
import com.unilever.ohub.spark.domain.entity.Activity
import com.unilever.ohub.spark.export.{Converter, InvertedBooleanToYNConverter}
import com.unilever.ohub.spark.export.acm.model.AcmActivity

object ActivityAcmConverter extends Converter[Activity, AcmActivity] with AcmTransformationFunctions {

  override def convert(activity: Activity): AcmActivity = {
    implicit val act: Activity = activity
    implicit val explain: Boolean = true

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

  def exampleConversion: (Activity, AcmActivity) = {
    val input = Activity(
      id = "id",
      creationTimestamp = Timestamp.valueOf("2015-05-01 12:00:00.0"),
      concatId = "concatId",
      countryCode = "countryCode",
      customerType = "customerType",
      dateCreated = Some(Timestamp.valueOf("2015-06-01 12:00:00.0")),
      dateUpdated = Some(Timestamp.valueOf("2015-07-01 12:00:00.0")),
      isActive = true,
      isGoldenRecord = true,
      sourceEntityId = "sourceEntityId",
      sourceName = "soruceName",
      ohubId = Some("ohubId"),
      ohubCreated = Timestamp.valueOf("2015-08-01 12:00:00.0"),
      ohubUpdated = Timestamp.valueOf("2015-09-01 12:00:00.0"),

      activityDate = Some(Timestamp.valueOf("2015-10-02 12:00:00.0")),
      name = Some("name"),
      details = Some("details"),
      actionType = Some("actionType"),
      contactPersonConcatId = Some("contactpersonConcatId"),
      contactPersonOhubId = Some("contactpersonsOhubId"),
      operatorConcatId = Some("operatorConcatId"),
      operatorOhubId = Some("operatorOhubId"),
      activityId = Some("activityId"),

      additionalFields = Map(),
      ingestionErrors = Map()
    )

    (input, convert(input))
  }
}

object A {
  def main(args: Array[String]): Unit = {
    val (input, output) = ActivityAcmConverter.exampleConversion
    println(DomainEntityUtils.prettyPrint(input))
    println("------------------")
    println(DomainEntityUtils.prettyPrint(output))
  }
}
