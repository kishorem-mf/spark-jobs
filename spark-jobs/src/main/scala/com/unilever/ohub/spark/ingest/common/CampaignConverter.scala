package com.unilever.ohub.spark.ingest.common

import com.unilever.ohub.spark.domain.entity.Campaign
import com.unilever.ohub.spark.ingest.CustomParsers._
import com.unilever.ohub.spark.ingest.{AnswerEmptyParquetWriter, DomainTransformer}
import org.apache.spark.sql.Row

object CampaignConverter extends CommonDomainGateKeeper[Campaign] with AnswerEmptyParquetWriter {

  override def toDomainEntity: DomainTransformer ⇒ Row ⇒ Campaign = { transformer ⇒row ⇒
    import transformer._
    implicit val source: Row = row

    val ohubCreated = currentTimestamp()

    // format: OFF

    Campaign(
      // fieldName                  mandatory                   sourceFieldName                                  targetFieldName                            transformationFunction (unsafe)
      id                          = mandatory(                  "id",                         "id"),
      creationTimestamp           = mandatory(                  "creationTimestamp",          "creationTimestamp",      toTimestamp),
      concatId                    = mandatory(                  "concatId",                   "concatId"),
      countryCode                 = mandatory(                  "countryCode",                "countryCode"),
      customerType                = mandatory(                  "customerType",               "customerType"),
      isActive                    = true,
      sourceEntityId              = mandatory(                  "sourceEntityId",             "sourceEntityId"),
      sourceName                  = mandatory(                  "sourceName",                 "sourceName"),
      ohubCreated                 = ohubCreated,
      ohubUpdated                 = ohubCreated,
      dateCreated                 = optional(                   "dateCreated",                "dateCreated",            parseDateTimeUnsafe()),
      dateUpdated                 = optional(                   "dateUpdated",                "dateUpdated",            parseDateTimeUnsafe()),
      ohubId                      = Option.empty,
      isGoldenRecord              = false,

      contactPersonConcatId       = mandatory(                  "contactPersonConcatId",      "contactPersonConcatId"),
      campaignId                  = mandatory(                  "campaignId",                 "campaignId"),
      campaignName                = mandatory(                  "campaignName",               "campaignName"),
      deliveryId                  = mandatory(                  "deliveryId",                 "deliveryId"),
      deliveryName                = mandatory(                  "deliveryName",               "deliveryName"),
      campaignSpecification       = mandatory(                  "campaignSpecification",      "campaignSpecification"),
      campaignWaveStartDate       = mandatory(                  "campaignWaveStartDate",      "campaignWaveStartDate",  parseDateTimeUnsafe()),
      campaignWaveEndDate         = mandatory(                  "campaignWaveEndDate",        "campaignWaveEndDate",    parseDateTimeUnsafe()),
      campaignWaveStatus          = mandatory(                  "campaignWaveStatus",         "campaignWaveStatus"),

      additionalFields            = additionalFields,
      ingestionErrors             = errors
    )
    // format: ON
  }
}
