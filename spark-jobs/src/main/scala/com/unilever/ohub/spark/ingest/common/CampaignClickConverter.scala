package com.unilever.ohub.spark.ingest.common

import com.unilever.ohub.spark.domain.entity.CampaignClick
import com.unilever.ohub.spark.ingest.CustomParsers._
import com.unilever.ohub.spark.ingest.{AnswerEmptyParquetWriter, DomainTransformer}
import org.apache.spark.sql.Row

object CampaignClickConverter extends CommonDomainGateKeeper[CampaignClick] with AnswerEmptyParquetWriter {

  override def toDomainEntity: DomainTransformer ⇒ Row ⇒ CampaignClick = { transformer ⇒row ⇒
    import transformer._
    implicit val source: Row = row

    val ohubCreated = currentTimestamp()

    // format: OFF

    CampaignClick(
      // fieldName                  mandatory                   sourceFieldName             targetFieldName                 transformationFunction (unsafe)
      id                          = mandatory(                  "id",                         "id"),
      creationTimestamp           = mandatory(                  "creationTimestamp",          "creationTimestamp", toTimestamp),
      concatId                    = mandatory(                  "concatId",                   "concatId"),
      countryCode                 = mandatory(                  "countryCode",                "countryCode"),
      customerType                = mandatory(                  "customerType",               "customerType"),
      isActive                    = true,
      sourceEntityId              = mandatory(                  "sourceEntityId",             "sourceEntityId"),
      sourceName                  = mandatory(                  "sourceName",                 "sourceName"),
      ohubCreated                 = ohubCreated,
      ohubUpdated                 = ohubCreated,
      dateCreated                 = optional(                   "dateCreated",                "dateCreated", parseDateTimeUnsafe()),
      dateUpdated                 = optional(                   "dateUpdated",                "dateUpdated", parseDateTimeUnsafe()),
      ohubId                      = Option.empty,
      isGoldenRecord              = true, // Not specified when is true in mapping, so always golden...

      trackingId                  = mandatory(                  "trackingId",                 "trackingId"),
      clickedUrl                  = mandatory(                  "clickedUrl",                 "clickedUrl"),
      clickDate                   = mandatory(                  "clickDate",                  "clickDate",   parseDateTimeUnsafe()),
      communicationChannel        = mandatory(                  "communicationChannel",       "communicationChannel"),
      campaignId                  = mandatory(                  "campaignId",                 "campaignId"),
      campaignName                = optional(                   "campaignName",               "campaignName"),
      deliveryId                  = mandatory(                  "deliveryId",                 "deliveryId"),
      deliveryName                = mandatory(                  "deliveryName",               "deliveryName"),
      contactPersonConcatId       = mandatory(                  "contactPersonConcatId",      "contactPersonConcatId"),
      contactPersonOhubId         = Option.empty,
      isOnMobileDevice            = mandatory(                  "isOnMobileDevice",           "isOnMobileDevice", toBoolean),
      operatingSystem             = optional(                   "operatingSystem",            "operatingSystem"),
      browserName                 = optional(                   "browserName",                "browserName"),
      browserVersion              = optional(                   "browserVersion",             "browserVersion"),
      operatorConcatId            = optional(                   "operatorConcatId",           "operatorConcatId"),
      operatorOhubId              = Option.empty,
      deliveryLogId               = mandatory(                  "deliveryLogId",              "deliveryLogId"),

      additionalFields            = additionalFields,
      ingestionErrors             = errors
    )
    // format: ON
  }
}
