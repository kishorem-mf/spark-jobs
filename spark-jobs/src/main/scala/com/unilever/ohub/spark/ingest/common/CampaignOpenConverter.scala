package com.unilever.ohub.spark.ingest.common

import com.unilever.ohub.spark.domain.entity.CampaignOpen
import com.unilever.ohub.spark.ingest.CustomParsers._
import com.unilever.ohub.spark.ingest.{AnswerEmptyParquetWriter, DomainTransformer}
import org.apache.spark.sql.Row

object CampaignOpenConverter extends CommonDomainGateKeeper[CampaignOpen] with AnswerEmptyParquetWriter {

  override def toDomainEntity: DomainTransformer ⇒ Row ⇒ CampaignOpen = { transformer ⇒row ⇒
    import transformer._
    implicit val source: Row = row

    val ohubCreated = currentTimestamp()

    // format: OFF

    CampaignOpen(
      // fieldName                  mandatory                   sourceFieldName                                 targetFieldName                        transformationFunction (unsafe)
      id                          = mandatory(                  "id",                         "id"),
      creationTimestamp           = mandatory(                  "creationTimestamp",          "creationTimestamp",    toTimestamp),
      concatId                    = mandatory(                  "concatId",                   "concatId"),
      countryCode                 = mandatory(                  "countryCode",                "countryCode"),
      customerType                = CampaignOpen.customerType,
      isActive                    = mandatory(                  "isActive",                   "isActive",             toBoolean),
      sourceEntityId              = mandatory(                  "sourceEntityId",             "sourceEntityId"),
      sourceName                  = mandatory(                  "sourceName",                 "sourceName"),
      ohubCreated                 = ohubCreated,
      ohubUpdated                 = ohubCreated,
      dateCreated                 = optional(                   "dateCreated",                "dateCreated",          parseDateTimeUnsafe()),
      dateUpdated                 = optional(                   "dateUpdated",                "dateUpdated",          parseDateTimeUnsafe()),
      ohubId                      = Option.empty,
      isGoldenRecord              = true, // Not specified when is true in mapping, so always golden...

      trackingId                  = mandatory(                  "trackingId",                 "trackingId"),
      campaignId                  = mandatory(                  "campaignId",                 "campaignId"),
      campaignName                = optional(                   "campaignName",               "campaignName"),
      deliveryId                  = mandatory(                  "deliveryId",                 "deliveryId"),
      deliveryName                = mandatory(                  "deliveryName",               "deliveryName"),
      communicationChannel        = mandatory(                  "communicationChannel",       "communicationChannel"),
      contactPersonConcatId       = mandatory(                  "contactPersonConcatId",      "contactPersonConcatId"),
      contactPersonOhubId         = Option.empty,
      operatorConcatId            = optional(                   "operatorConcatId",           "operatorConcatId"),
      operatorOhubId              = Option.empty,
      openDate                    = mandatory(                  "openDate",                   "openDate",             parseDateTimeUnsafe()),
      deliveryLogId               = mandatory(                  "deliveryLogId",              "deliveryLogId"),

      additionalFields            = additionalFields,
      ingestionErrors             = errors
    )

    // format: ON
  }
}
