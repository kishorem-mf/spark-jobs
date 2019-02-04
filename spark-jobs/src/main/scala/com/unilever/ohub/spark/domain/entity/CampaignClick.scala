package com.unilever.ohub.spark.domain.entity

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.domain.DomainEntity.IngestionError

object CampaignClick {
  val customerType = "CONTACTPERSON"
}

case class CampaignClick(
    // generic fields
    // mandatory fields
    id: String,
    creationTimestamp: Timestamp,
    concatId: String,
    countryCode: String,
    customerType: String,
    sourceEntityId: String,
    campaignConcatId: String,
    sourceName: String,
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
    trackingId: String,
    clickedUrl: String,
    clickDate: Timestamp,
    communicationChannel: String,
    campaignId: String,
    campaignName: Option[String],
    deliveryId: String,
    deliveryName: String,
    contactPersonConcatId: String,
    contactPersonOhubId: Option[String],
    isOnMobileDevice: Boolean,
    operatingSystem: Option[String],
    browserName: Option[String],
    browserVersion: Option[String],
    operatorConcatId: Option[String],
    operatorOhubId: Option[String],
    deliveryLogId: Option[String],

    // other fields
    additionalFields: Map[String, String],
    ingestionErrors: Map[String, IngestionError]
) extends DomainEntity {}
