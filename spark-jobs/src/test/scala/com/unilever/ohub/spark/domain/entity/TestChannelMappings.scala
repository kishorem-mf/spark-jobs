package com.unilever.ohub.spark.domain.entity

import java.sql.Timestamp

trait TestChannelMappings {

  lazy val defaultChannelReference = ChannelReference(
    channelReferenceId = "1",
    globalChannel = "GLOBAL_CHANNEL",
    globalSubChannel = "GLOBAL_SUBCHANNEL",
    socialCommercial = Some("SOCIAL_COMMERCIAL"),
    strategicChannel = "STRATEGIC_CHANNEL"
  )

  lazy val defaultChannelMapping = ChannelMapping(
    id = "id-1",
    creationTimestamp = new Timestamp(1542205922011L),
    concatId = "DE~EMAKINA~b3a6208c-d7f6-44e2-80e2-f26d461f64c0",
    countryCode = "DE",
    customerType = "CONTACTPERSON",
    dateCreated = None,
    dateUpdated = None,
    isActive = true,
    isGoldenRecord = false,
    sourceEntityId = "b3a6208c-d7f6-44e2-80e2-f26d461f64c0",
    sourceName = "EMAKINA",
    ohubId = None,
    ohubCreated = Timestamp.valueOf("2015-06-30 13:49:00.0"),
    ohubUpdated = Timestamp.valueOf("2015-06-30 13:49:00.0"),

    originalChannel = "ORIGIN",
    localChannel = "LOCAL",
    channelUsage = "YES",
    channelReference = "1",

    additionalFields = Map(),
    ingestionErrors = Map()
  )
}
