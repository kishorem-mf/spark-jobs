package com.unilever.ohub.spark.ingest

import com.unilever.ohub.spark.DomainDataProvider
import com.unilever.ohub.spark.domain.entity.ChannelReference

case class TestDomainDataProvider(
                                   sourcePreferences: Map[String, Int] = Map(
                                     "WUFOO" -> 1,
                                     "EMAKINA" -> 2,
                                     "FUZZIT" -> 3,
                                     "SIFU" -> 4,
                                     "WEB_EVENT" -> 5,
                                     "KANGAROO" -> 6
                                   ),

                                   channelReferences: Map[String, ChannelReference] = Map[String, ChannelReference](
                                     "1" -> ChannelReference(
                                       channelReferenceId = "1",
                                       globalChannel = "GLOBAL_CHANNEL",
                                       globalSubChannel = "GLOBAL_SUBCHANNEL",
                                       socialCommercial = Some("SOCIAL_COMMERCIAL"),
                                       strategicChannel = "STRATEGIC_CHANNEL"
                                     )
                                   ),

                                   sourceIds: Map[String, Int] = Map(
                                     "FILE" -> 1,
                                     "WEBUPDATER" -> 2,
                                     "FUZZIT" -> 3,
                                     "DEX" -> 4
                                   ),

                                   salesOrgToCountryMap: Map[String, String] = Map(
                                     "1300" -> "DE",
                                     "4050" -> "PL",
                                     "5220" -> "FI",
                                     "1700" -> "IT"
                                   )
                                 ) extends DomainDataProvider
