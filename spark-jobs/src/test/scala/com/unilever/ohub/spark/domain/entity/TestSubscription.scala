package com.unilever.ohub.spark.domain.entity

import java.sql.Timestamp

object TestSubscription extends TestSubscription

trait TestSubscription {

  lazy val defaultSubscription: Subscription = Subscription(
    id = "id-1",
    creationTimestamp = new Timestamp(1542205922011L),
    concatId = "DE~EMAKINA~b3a6208c-d7f6-44e2-80e2-f26d461f64c0",
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
    contactPersonOhubId = None,
    communicationChannel = Some("all"),
    subscriptionType = "Newsletter",
    hasSubscription = true,
    subscriptionDate = Some(Timestamp.valueOf("2015-06-20 13:49:00.0")),
    hasConfirmedSubscription = Some(true),
    confirmedSubscriptionDate = Some(Timestamp.valueOf("2015-06-30 13:49:00.0")),
    fairKitchensSignUpType = Some("pledger"),
    //BDL Fields
    newsletterNumber= None,
    createdBy= None,
    currency= None,
    hasPricingInfo= None,
    language= None,
    name= None,
    owner= None,
    numberOfTimesSent= None,
    localOrGlobalSendOut= None,
    comment= None,
    // other fields
    additionalFields = Map(),
    ingestionErrors = Map()
  )
}
