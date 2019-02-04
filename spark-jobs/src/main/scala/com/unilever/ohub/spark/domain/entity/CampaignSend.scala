package com.unilever.ohub.spark.domain.entity

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.domain.DomainEntity.IngestionError

object CampaignSend {
  val customerType = "CONTACTPERSON"
}

case class CampaignSend(
    // generic fields
    // mandatory fields
    id: String,
    creationTimestamp: Timestamp,
    concatId: String,
    countryCode: String,
    customerType: String,
    sourceEntityId: String,
    sourceName: String,
    campaignConcatId: String,
    isActive: Boolean,
    ohubCreated: Timestamp,
    ohubUpdated: Timestamp,
    // optional fields
    dateCreated: Option[Timestamp],
    dateUpdated: Option[Timestamp],
    // used for grouping and marking the golden record within the group
    ohubId: Option[String],
    isGoldenRecord: Boolean,

    // Specific fields
    deliveryLogId: String,
    campaignId: String,
    campaignName: Option[String],
    deliveryId: String,
    deliveryName: String,
    communicationChannel: String,
    operatorConcatId: Option[String],
    operatorOhubId: Option[String],
    sendDate: Timestamp,
    isControlGroupMember: Boolean,
    isProofGroupMember: Boolean,
    selectionForOfflineChannels: String,
    contactPersonConcatId: String,
    contactPersonOhubId: Option[String],
    waveName: String,

    // other fields
    additionalFields: Map[String, String],
    ingestionErrors: Map[String, IngestionError]
) extends DomainEntity {}
