package com.unilever.ohub.spark.domain.entity

import java.sql.Timestamp

trait TestCampaignBounces {

  lazy val defaultCampaignBounce: CampaignBounce = CampaignBounce(
    id = "id-1",
    creationTimestamp = new Timestamp(1542205922011L),
    concatId = "b3a6208c~NL~EMAKINA~1003499146~2018-10-08T22:53:51",
    countryCode = "NL",
    customerType = "CONTACTPERSON",
    isActive = true,
    sourceEntityId = "b3a6208c~NL~EMAKINA~1003499146",
    campaignConcatId = "b3a6208c~NL~EMAKINA~1003499146~f26d461f64c0",
    sourceName = "ACM",
    ohubCreated = Timestamp.valueOf("2015-06-30 13:49:00.0"),
    ohubUpdated = Timestamp.valueOf("2015-06-30 13:49:00.0"),
    dateCreated = None,
    dateUpdated = None,
    ohubId = null,
    isGoldenRecord = false,

    deliveryLogId = "6605058",
    campaignId = "9687671",
    campaignName = Some("UFS00D5033a_01_Spargel_2016_Q1_AT"),
    deliveryId = "66645132",
    deliveryName = "Email_Loyalists",
    communicationChannel = "Email",
    contactPersonConcatId = Some("AT~OHUB~529541"),
    contactPersonOhubId = "529541",
    bounceDate = Timestamp.valueOf("2016-04-17 18:00:30.0"),
    failureType = "Hard",
    failureReason = "User unknown",
    isControlGroupMember = false,
    isProofGroupMember = false,
    operatorConcatId = Some("AT~OHUB~5295411"),
    operatorOhubId = Some("5295411"),

    additionalFields = Map(),
    ingestionErrors = Map()
  )
}
