package com.unilever.ohub.spark.domain.entity

import java.sql.Timestamp

trait TestCampaignClicks {

  lazy val defaultCampaignClick: CampaignClick = CampaignClick(
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

    trackingId = "2695121",
    clickedUrl = "https://www.dasgrossestechen.at/gewinnspiel",
    clickDate = Timestamp.valueOf("2015-06-30 13:49:00.0"),
    communicationChannel = "Email",
    campaignId = "64431645",
    campaignName = Some("MW_DDNL_MAERZ_CMP_PROD_AT"),
    deliveryId = "64431647",
    deliveryName = "Copy of DFR_DDNL_DELIVERY",
    contactPersonConcatId = "AT~210946~3~6",
    isOnMobileDevice = false,
    operatingSystem = Option.empty,
    browserName = Option.empty,
    browserVersion = Option.empty,
    operatorConcatId = Some("AT~MM-INITOPER~AT~210946~1~6"),
    deliveryLogId = "6605058",

    additionalFields = Map(),
    ingestionErrors = Map()
  )
}
