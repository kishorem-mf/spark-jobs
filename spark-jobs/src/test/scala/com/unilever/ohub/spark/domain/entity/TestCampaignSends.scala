package com.unilever.ohub.spark.domain.entity

import java.sql.Timestamp

trait TestCampaignSends {

  lazy val defaultCampaignSend: CampaignSend = CampaignSend(
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

    deliveryLogId = "6160583",
    campaignId = "65054561",
    campaignName = Some("20160324 - Lipton"),
    deliveryId = "65096985",
    deliveryName = "NLLipton032016 -- 20160323 -- proofed",
    communicationChannel = "Email",
    operatorConcatId = Some("NL~OHUB~10037250061"),
    operatorOhubId = Some("10037250061"),
    sendDate = Timestamp.valueOf("2016-03-28 21:16:50.0"),
    isControlGroupMember = false,
    isProofGroupMember = true,
    selectionForOfflineChannels = "24",
    contactPersonConcatId = Some("NL~OHUB~1003499146"),
    contactPersonOhubId = "1003499146",
    waveName = "20160324 - Lipton~NLLipton032016 -- 20160323 -- proofed",

    additionalFields = Map(),
    ingestionErrors = Map()
  )
}
