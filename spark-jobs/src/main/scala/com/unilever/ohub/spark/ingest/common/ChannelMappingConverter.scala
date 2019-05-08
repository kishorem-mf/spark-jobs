package com.unilever.ohub.spark.ingest.common

import com.unilever.ohub.spark.domain.entity.ChannelMapping
import com.unilever.ohub.spark.ingest.CustomParsers._
import com.unilever.ohub.spark.ingest.{ ActivityEmptyParquetWriter, DomainTransformer }
import org.apache.spark.sql.Row

object ChannelMappingConverter extends CommonDomainGateKeeper[ChannelMapping] with ActivityEmptyParquetWriter {

  override def toDomainEntity: DomainTransformer ⇒ Row ⇒ ChannelMapping = { transformer ⇒ row ⇒
    import transformer._
    implicit val source: Row = row

    val ohubCreated = currentTimestamp()

    // format: OFF

    ChannelMapping(
      // fieldName                  mandatory                   sourceFieldName             targetFieldName                 transformationFunction (unsafe)
      id                          = mandatory(                  "id",                         "id"),
      creationTimestamp           = mandatory(                  "creationTimestamp",          "creationTimestamp", toTimestamp),
      concatId                    = mandatory(                  "concatId",                   "concatId"),
      countryCode                 = mandatory(                  "countryCode",                "countryCode"),
      customerType                = ChannelMapping.customerType,
      dateCreated                 = None,
      dateUpdated                 = None,
      isActive                    = true,
      isGoldenRecord              = true,
      sourceEntityId              = mandatory(                  "sourceEntityId",             "sourceEntityId"),
      sourceName                  = mandatory(                  "sourceName",                 "sourceName"),
      ohubId                      = Option.empty,
      ohubCreated                 = ohubCreated,
      ohubUpdated                 = ohubCreated,

      originalChannel             = mandatory(                  "originalChannel",            "originalChannel"),
      localChannel                = mandatory(                  "localChannel",               "localChannel"),
      channelUsage                = mandatory(                  "channelUsage",               "channelUsage"),
      channelReference            = mandatory(                  "channelReference",           "channelReference"),

      additionalFields            = additionalFields,
      ingestionErrors             = errors
    )

    // format: ON
  }
}
