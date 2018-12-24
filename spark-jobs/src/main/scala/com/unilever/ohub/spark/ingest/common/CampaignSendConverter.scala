package com.unilever.ohub.spark.ingest.common

import com.unilever.ohub.spark.domain.entity.CampaignSend
import com.unilever.ohub.spark.ingest.CustomParsers._
import com.unilever.ohub.spark.ingest.{AnswerEmptyParquetWriter, DomainTransformer}
import org.apache.spark.sql.Row

object CampaignSendConverter extends CommonDomainGateKeeper[CampaignSend] with AnswerEmptyParquetWriter {

  override def toDomainEntity: DomainTransformer ⇒ Row ⇒ CampaignSend = { transformer ⇒row ⇒
    import transformer._
    implicit val source: Row = row

    val ohubCreated = currentTimestamp()

    // format: OFF

    CampaignSend(
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
      isGoldenRecord              = false,

      deliveryLogId               = mandatory(                  "deliveryLogId",              "deliveryLogId"),
      campaignId                  = mandatory(                  "campaignId",                 "campaignId"),
      campaignName                = optional(                   "campaignName",               "campaignName"),
      deliveryId                  = mandatory(                  "deliveryId",                 "deliveryId"),
      deliveryName                = mandatory(                  "deliveryName",               "deliveryName"),
      communicationChannel        = mandatory(                  "communicationChannel",       "communicationChannel"),
      operatorConcatId            = optional(                   "operatorConcatId",           "operatorConcatId"),
      sendDate                    = mandatory(                  "sendDate",                   "sendDate", parseDateTimeUnsafe()),
      isControlGroupMember        = mandatory(                  "isControlGroupMember",       "isControlGroupMember", toBoolean),
      isProofGroupMember          = mandatory(                  "isProofGroupMember",         "isProofGroupMember", toBoolean),
      selectionForOfflineChannels = mandatory(                  "selectionForOfflineChannels","selectionForOfflineChannels"),
      contactPersonConcatId       = mandatory(                  "contactPersonOhubId",        "contactPersonOhubId"),
      waveName                    = mandatory(                  "waveName",                   "waveName"),

      additionalFields            = additionalFields,
      ingestionErrors             = errors
    )
    // format: ON
  }
}
