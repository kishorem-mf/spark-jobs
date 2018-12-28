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
      // fieldName                  mandatory                   sourceFieldName                                 targetFieldName                        transformationFunction (unsafe)
      id                          = mandatory(                  "id",                         "id"),
      creationTimestamp           = mandatory(                  "creationTimestamp",          "creationTimestamp",    toTimestamp),
      concatId                    = mandatory(                  "concatId",                   "concatId"),
      countryCode                 = mandatory(                  "countryCode",                "countryCode"),
      customerType                = CampaignSend.customerType,
      isActive                    = mandatory(                  "isActive",                   "isActive",             toBoolean),
      sourceEntityId              = mandatory(                  "sourceEntityId",             "sourceEntityId"),
      sourceName                  = mandatory(                  "sourceName",                 "sourceName"),
      ohubCreated                 = ohubCreated,
      ohubUpdated                 = ohubCreated,
      dateCreated                 = optional(                   "dateCreated",                "dateCreated",          parseDateTimeUnsafe()),
      dateUpdated                 = optional(                   "dateUpdated",                "dateUpdated",          parseDateTimeUnsafe()),
      ohubId                      = Option.empty,
      isGoldenRecord              = true, // Not specified when is true in mapping, so always golden...

      deliveryLogId               = mandatory(                  "deliveryLogId",              "deliveryLogId"),
      campaignId                  = mandatory(                  "campaignId",                 "campaignId"),
      campaignName                = optional(                   "campaignName",               "campaignName"),
      deliveryId                  = mandatory(                  "deliveryId",                 "deliveryId"),
      deliveryName                = mandatory(                  "deliveryName",               "deliveryName"),
      communicationChannel        = mandatory(                  "communicationChannel",       "communicationChannel"),
      operatorConcatId            = optional(                   "operatorConcatId",           "operatorConcatId"),
      operatorOhubId              = Option.empty,
      sendDate                    = mandatory(                  "sendDate",                   "sendDate",             parseDateTimeUnsafe()),
      isControlGroupMember        = mandatory(                  "isControlGroupMember",       "isControlGroupMember", toBoolean),
      isProofGroupMember          = mandatory(                  "isProofGroupMember",         "isProofGroupMember",   toBoolean),
      selectionForOfflineChannels = mandatory(                  "selectionForOfflineChannels","selectionForOfflineChannels"),
      contactPersonConcatId       = mandatory(                  "contactPersonConcatId",        "contactPersonConcatId"),
      contactPersonOhubId         = Option.empty,
      waveName                    = mandatory(                  "waveName",                   "waveName"),

      additionalFields            = additionalFields,
      ingestionErrors             = errors
    )
    // format: ON
  }
}
