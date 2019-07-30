package com.unilever.ohub.spark.ingest.common

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.entity.CampaignBounce
import com.unilever.ohub.spark.ingest.CustomParsers._
import com.unilever.ohub.spark.ingest.{CampaignBounceEmptyParquetWriter, DomainTransformer}
import org.apache.spark.sql.Row
object CampaignBounceConverter extends CommonDomainGateKeeper[CampaignBounce] with CampaignBounceEmptyParquetWriter {

  override def toDomainEntity: DomainTransformer ⇒ Row ⇒ CampaignBounce = { transformer ⇒ row ⇒
    import transformer._
    implicit val source: Row = row

    val ohubCreated = new Timestamp(System.currentTimeMillis())

    // format: OFF

    CampaignBounce(
      // fieldName                  mandatory                   sourceFieldName                                 targetFieldName                        transformationFunction (unsafe)
      id                          = mandatory(                  "id"),
      creationTimestamp           = mandatory(                  "creationTimestamp",    toTimestamp),
      concatId                    = mandatory(                  "concatId"),
      countryCode                 = mandatory(                  "countryCode"),
      customerType                = CampaignBounce.customerType,
      isActive                    = mandatory(                  "isActive",             toBoolean),
      sourceEntityId              = mandatory(                  "sourceEntityId"),
      campaignConcatId            = mandatory(                  "campaignConcatId"),
      sourceName                  = mandatory(                  "sourceName"),
      ohubCreated                 = ohubCreated,
      ohubUpdated                 = ohubCreated,
      dateCreated                 = optional(                   "dateCreated",          parseDateTimeUnsafe()),
      dateUpdated                 = optional(                   "dateUpdated",          parseDateTimeUnsafe()),
      ohubId                      = Option.empty,
      isGoldenRecord              = true, // Not specified when is true in mapping, so always golden...

      deliveryLogId               = mandatory(                  "deliveryLogId"),
      campaignId                  = mandatory(                  "campaignId"),
      campaignName                = optional(                   "campaignName"),
      deliveryId                  = mandatory(                  "deliveryId"),
      deliveryName                = mandatory(                  "deliveryName"),
      communicationChannel        = mandatory(                  "communicationChannel"),
      contactPersonConcatId       = mandatory(                  "contactPersonConcatId"),
      contactPersonOhubId         = Option.empty,
      bounceDate                  = mandatory(                  "bounceDate",           parseDateTimeUnsafe()),
      failureType                 = mandatory(                  "failureType"),
      failureReason               = mandatory(                  "failureReason"),
      isControlGroupMember        = mandatory(                  "isControlGroupMember",toBoolean),
      isProofGroupMember          = mandatory(                  "isProofGroupMember",  toBoolean),
      operatorConcatId            = optional(                   "operatorConcatId"),
      operatorOhubId              = Option.empty,

      additionalFields            = additionalFields,
      ingestionErrors             = errors
    )
    // format: ON
  }
}
