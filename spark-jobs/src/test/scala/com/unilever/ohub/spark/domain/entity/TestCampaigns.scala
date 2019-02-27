package com.unilever.ohub.spark.domain.entity

import java.sql.Timestamp

trait TestCampaigns {

  lazy val defaultCampaign: Campaign = Campaign(
    id = "id-1",
    creationTimestamp = new Timestamp(1542205922011L),
    concatId = "b3a6208c~NL~EMAKINA~1003499146~2018-10-08T22:53:51",
    countryCode = "NL",
    customerType = "CONTACTPERSON",
    isActive = true,
    sourceEntityId = "b3a6208c~NL~EMAKINA~1003499146",
    campaignConcatId = "b3a6208c~NL~EMAKINA~1003499146",
    sourceName = "ACM",
    ohubCreated = Timestamp.valueOf("2015-06-30 13:49:00.0"),
    ohubUpdated = Timestamp.valueOf("2015-06-30 13:49:00.0"),
    dateCreated = None,
    dateUpdated = None,
    ohubId = null,
    isGoldenRecord = false,

    contactPersonConcatId = "DE~ACM~b3a6208c-d7f6-44e2-80e2-f26d461f64c1",
    contactPersonOhubId = Option.empty,
    campaignId = "65054561",
    campaignName = Some("20160324 - Lipton"),
    deliveryId = "65054561",
    deliveryName = "NLLipton032016 --20160325 – followup",
    campaignSpecification = "Product Introduction",
    campaignWaveStartDate = Timestamp.valueOf("2017-09-28 13:49:00.0"),
    campaignWaveEndDate = Timestamp.valueOf("2017-09-28 13:50:00.0"),
    campaignWaveStatus = "ended",

    additionalFields = Map(),
    ingestionErrors = Map()
  )
}
