package com.unilever.ohub.spark.ingest.common

import com.unilever.ohub.spark.domain.entity.Campaign
import com.unilever.ohub.spark.ingest.CustomParsers._
import com.unilever.ohub.spark.ingest.{ CampaignEmptyParquetWriter, DomainTransformer }
import org.apache.spark.sql.Row

object CampaignConverter extends CommonDomainGateKeeper[Campaign] with CampaignEmptyParquetWriter {

  override def toDomainEntity: DomainTransformer ⇒ Row ⇒ Campaign = { transformer ⇒ row ⇒
    import transformer._
    implicit val source: Row = row

    val ohubCreated = currentTimestamp()

    // format: OFF

    Campaign(
      // fieldName                  mandatory                   sourceFieldName                                 targetFieldName                        transformationFunction (unsafe)
      id                          = mandatory(                  "id",                         "id"),
      creationTimestamp           = mandatory(                  "creationTimestamp",          "creationTimestamp",    toTimestamp),
      concatId                    = mandatory(                  "concatId",                   "concatId"),
      countryCode                 = mandatory(                  "countryCode",                "countryCode"),
      customerType                = Campaign.customerType,
      isActive                    = mandatory(                  "isActive",                   "isActive",             toBoolean),
      sourceEntityId              = mandatory(                  "sourceEntityId",             "sourceEntityId"),
      sourceName                  = mandatory(                  "sourceName",                 "sourceName"),
      ohubCreated                 = ohubCreated,
      ohubUpdated                 = ohubCreated,
      dateCreated                 = optional(                   "dateCreated",                "dateCreated",          parseDateTimeUnsafe()),
      dateUpdated                 = optional(                   "dateUpdated",                "dateUpdated",          parseDateTimeUnsafe()),
      ohubId                      = Option.empty,
      isGoldenRecord              = true, // Not specified when is true in mapping, so always golden...

      contactPersonConcatId       = mandatory(                  "contactPersonConcatId",      "contactPersonConcatId"),
      contactPersonOhubId         = Option.empty,
      campaignId                  = mandatory(                  "campaignId",                 "campaignId"),
      campaignName                = mandatory(                  "campaignName",               "campaignName"),
      deliveryId                  = mandatory(                  "deliveryId",                 "deliveryId"),
      deliveryName                = mandatory(                  "deliveryName",               "deliveryName"),
      campaignSpecification       = mandatory(                  "campaignSpecification",      "campaignSpecification"),
      campaignWaveStartDate       = mandatory(                  "campaignWaveStartDate",      "campaignWaveStartDate",parseDateTimeUnsafe()),
      campaignWaveEndDate         = mandatory(                  "campaignWaveEndDate",        "campaignWaveEndDate",  parseDateTimeUnsafe()),
      campaignWaveStatus          = mandatory(                  "campaignWaveStatus",         "campaignWaveStatus"),

      additionalFields            = additionalFields,
      ingestionErrors             = errors
    )
    // format: ON
  }
}
