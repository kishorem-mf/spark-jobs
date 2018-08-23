package com.unilever.ohub.spark.domain.entity

import java.sql.Timestamp

object TestSubscription extends TestSubscription

trait TestSubscription {

  lazy val defaultSubscription: Subscription = Subscription(
    concatId = "DE~SUBSCRIPTION~b3a6208c-d7f6-44e2-80e2-f26d461f64c0",
    countryCode = "DE",
    customerType = "SUBSCRIPTION",
    dateCreated = None,
    dateUpdated = None,
    isActive = true,
    isGoldenRecord = false,
    sourceEntityId = "b3a6208c-d7f6-44e2-80e2-f26d461f64c0",
    sourceName = "EMAKINA",
    ohubId = null,
    ohubCreated = Timestamp.valueOf("2015-06-30 13:49:00.0"),
    ohubUpdated = Timestamp.valueOf("2015-06-30 13:49:00.0"),
    contactPersonConcatId = "DE~SUBSCRIPTION~138175",
    communicationChannel = Some("Some channel"),
    subscriptionType = "Newsletter",
    hasSubscription = true,
    subscriptionDate = Timestamp.valueOf("2015-06-20 13:49:00.0"),
    hasConfirmedSubscription = Some(true),
    ConfirmedSubscriptionDate = Some(Timestamp.valueOf("2015-06-30 13:49:00.0")),
    // other fields
    additionalFields = Map(),
    ingestionErrors = Map()

  )
}
