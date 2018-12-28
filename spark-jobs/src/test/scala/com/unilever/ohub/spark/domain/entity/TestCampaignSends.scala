package com.unilever.ohub.spark.domain.entity

import java.sql.Timestamp

trait TestCampaignSends {

  lazy val defaultCampaignSend: CampaignSend = CampaignSend(
    id = "id-1",
    creationTimestamp = new Timestamp(1542205922011L),
    concatId = "NL~1003499146~1003",
    countryCode = "NL",
    customerType = "CONTACTPERSON",
    isActive = true,
    sourceEntityId = "b3a6208c-d7f6-44e2-80e2-f26d461f64c0",
    sourceName = "ACM",
    ohubCreated = Timestamp.valueOf("2015-06-30 13:49:00.0"),
    ohubUpdated = Timestamp.valueOf("2015-06-30 13:49:00.0"),
    dateCreated = None,
    dateUpdated = None,
    ohubId = null,
    isGoldenRecord = false,

    deliveryLogId = "6160583",
    campaignId = "65054561",
    campaignName = Some("20160324 - Lipton"),
    deliveryId = "65096985",
    deliveryName = "NLLipton032016 -- 20160323 -- proofed",
    communicationChannel = "Email",
    operatorConcatId = Some("NL~1003725006~1~4"),
    operatorOhubId = Option.empty,
    sendDate = Timestamp.valueOf("2016-03-28 21:16:50.0"),
    isControlGroupMember = false,
    isProofGroupMember = true,
    selectionForOfflineChannels = "24",
    contactPersonConcatId = "NL~1003499146~10037~25006~ULNL~3~4",
    contactPersonOhubId = Option.empty,
    waveName = "20160324 - Lipton~NLLipton032016 -- 20160323 -- proofed",

    additionalFields = Map(),
    ingestionErrors = Map()
  )
}
