package com.unilever.ohub.spark.domain.entity

import java.sql.Timestamp

trait TestCampaignOpens {

  lazy val defaultCampaignOpen: CampaignOpen = CampaignOpen(
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

    trackingId = "2691000",
    campaignId = "64463894",
    campaignName = Some("MW_DDNL_MAERZ_CMP_PROD_CH"),
    deliveryId = "64463896",
    deliveryName = "Copy of DFR_DDNL_DELIVERY",
    communicationChannel = "Email",
    contactPersonConcatId = Some("CH~OHUB~1003015423"),
    contactPersonOhubId = "1003015423",
    operatorConcatId = Some("CH~OHUB~5734521"),
    operatorOhubId = Some("5734521"),
    openDate = Timestamp.valueOf("2016-04-17 18:00:30.0"),
    deliveryLogId = Some("6605058"),

    additionalFields = Map(),
    ingestionErrors = Map()
  )
}
