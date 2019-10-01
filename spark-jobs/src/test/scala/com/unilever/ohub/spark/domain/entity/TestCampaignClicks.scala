package com.unilever.ohub.spark.domain.entity

import java.sql.Timestamp

trait TestCampaignClicks {

  lazy val defaultCampaignClick: CampaignClick = CampaignClick(
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

    trackingId = "2695121",
    clickedUrl = "https://www.dasgrossestechen.at/gewinnspiel",
    clickDate = Timestamp.valueOf("2015-06-30 13:49:00.0"),
    communicationChannel = "Email",
    campaignId = "64431645",
    campaignName = Some("MW_DDNL_MAERZ_CMP_PROD_AT"),
    deliveryId = "64431647",
    deliveryName = "Copy of DFR_DDNL_DELIVERY",
    contactPersonConcatId = Some("AT~OHUB~210946"),
    contactPersonOhubId = "210946",
    isOnMobileDevice = false,
    operatingSystem = Option.empty,
    browserName = Option.empty,
    browserVersion = Option.empty,
    operatorConcatId = Some("AT~OHUB~2109461"),
    operatorOhubId = Some("2109461"),
    deliveryLogId = Option("6605058"),

    additionalFields = Map(),
    ingestionErrors = Map()
  )
}
