package com.unilever.ohub.spark.ingest.common

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.entity.ChannelMapping
import com.unilever.ohub.spark.ingest.CustomParsers._
import com.unilever.ohub.spark.ingest.{ActivityEmptyParquetWriter, DomainTransformer}
import org.apache.spark.sql.Row

object ChannelMappingConverter extends CommonDomainGateKeeper[ChannelMapping] with ActivityEmptyParquetWriter {

  override def toDomainEntity: DomainTransformer ⇒ Row ⇒ ChannelMapping = { transformer ⇒
    row ⇒
      import transformer._
      implicit val source: Row = row

      val ohubCreated = new Timestamp(System.currentTimeMillis())

      ChannelMapping(
        id = mandatory("id"),
        creationTimestamp = mandatory("creationTimestamp", toTimestamp),
        concatId = mandatory("concatId"),
        countryCode = mandatory("countryCode"),
        customerType = ChannelMapping.customerType,
        dateCreated = None,
        dateUpdated = None,
        isActive = true,
        isGoldenRecord = true,
        sourceEntityId = mandatory("sourceEntityId"),
        sourceName = mandatory("sourceName"),
        ohubId = Option.empty,
        ohubCreated = ohubCreated,
        ohubUpdated = ohubCreated,

        originalChannel = mandatory("originalChannel"),
        localChannel = mandatory("localChannel"),
        channelUsage = mandatory("channelUsage"),
        channelReference = mandatory("channelReference"),

        additionalFields = additionalFields,
        ingestionErrors = errors
      )
  }
}
