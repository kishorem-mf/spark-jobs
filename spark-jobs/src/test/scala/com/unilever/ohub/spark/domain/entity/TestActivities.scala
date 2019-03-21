package com.unilever.ohub.spark.domain.entity

import java.sql.Timestamp

object TestActivities extends TestActivities

trait TestActivities {

  lazy val defaultActivity: Activity = Activity(
    id = "id-1",
    creationTimestamp = new Timestamp(1542205922011L),
    concatId = "DE~EMAKINA~b3a6208c-d7f6-44e2-80e2-f26d461f64c0",
    countryCode = "DE",
    customerType = "CONTACTPERSON",
    dateCreated = None,
    dateUpdated = None,
    isActive = true,
    isGoldenRecord = false,
    sourceEntityId = "b3a6208c-d7f6-44e2-80e2-f26d461f64c0",
    sourceName = "EMAKINA",
    ohubId = null,
    ohubCreated = Timestamp.valueOf("2015-06-30 13:49:00.0"),
    ohubUpdated = Timestamp.valueOf("2015-06-30 13:49:00.0"),

    activityDate = None,
    name = Some("some name"),
    details = Some("some details"),
    actionType = Some("MyActionType"),
    contactPersonConcatId = Some("DE~SUBSCRIPTION~138175"),
    contactPersonOhubId = None,
    operatorConcatId = Some("DE~SUBSCRIPTION~138175"),
    operatorOhubId = None,
    activityId = Some("232323"),

    additionalFields = Map(),
    ingestionErrors = Map()
  )
}
