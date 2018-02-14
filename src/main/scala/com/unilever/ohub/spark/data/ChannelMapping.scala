package com.unilever.ohub.spark.data

case class ChannelMapping(
  countryCode: String,
  localChannel: String,
  channelUsage: String,
  socialCommercial: String,
  strategicChannel: String,
  globalChannel: String,
  globalSubChannel: String
)
