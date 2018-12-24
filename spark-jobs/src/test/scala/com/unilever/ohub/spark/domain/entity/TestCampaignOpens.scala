package com.unilever.ohub.spark.domain.entity

import java.sql.Timestamp

trait TestCampaignOpens {

  lazy val defaultCampaign: CampaignOpen = CampaignOpen(
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

    trackingId = "2691000",
    campaignId = "64463894",
    campaignName = Some("MW_DDNL_MAERZ_CMP_PROD_CH"),
    deliveryId = "64463896",
    deliveryName = "Copy of DFR_DDNL_DELIVERY",
    communicationChannel = "Email",
    contactPersonConcatId = "CH~1003015423~573452~ULCH~3~4",
    operatorConcatId = Some("CH~573452~1~4"),
    openDate = Timestamp.valueOf("2016-04-17 18:00:30.0"),
    deliveryLogId = "6605058",

    additionalFields = Map(),
    ingestionErrors = Map()
  )
}
