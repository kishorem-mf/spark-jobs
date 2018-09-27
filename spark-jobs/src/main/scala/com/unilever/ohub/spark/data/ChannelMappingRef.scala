package com.unilever.ohub.spark.data

case class ChannelMappingRef(
    countryCode: String,
    originalChannel: String,
    localChannel: String,
    channelUsage: String,
    channelReferenceFk: String
)
