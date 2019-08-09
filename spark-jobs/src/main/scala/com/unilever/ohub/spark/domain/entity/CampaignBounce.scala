package com.unilever.ohub.spark.domain.entity

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.DomainEntity.IngestionError
import com.unilever.ohub.spark.domain.{DomainEntity, DomainEntityCompanion}
import com.unilever.ohub.spark.export.ExportOutboundWriter
import com.unilever.ohub.spark.export.azuredw.{AzureDWWriter, CampaignBounceDWWriter}
import com.unilever.ohub.spark.export.dispatch.CampaignBounceOutboundWriter
import com.unilever.ohub.spark.export.domain.DomainExportWriter

object CampaignBounceDomainExportWriter extends DomainExportWriter[CampaignBounce]

object CampaignBounce extends DomainEntityCompanion {
  val customerType = "CONTACTPERSON"
  override val engineFolderName = "campaignbounces"
  override val domainExportWriter: Option[DomainExportWriter[CampaignBounce]] = Some(CampaignBounceDomainExportWriter)
  override val acmExportWriter: Option[ExportOutboundWriter[CampaignBounce]] = None
  override val dispatchExportWriter: Option[ExportOutboundWriter[CampaignBounce]] = Some(CampaignBounceOutboundWriter)
  override val azureDwWriter: Option[AzureDWWriter[CampaignBounce]] = Some(CampaignBounceDWWriter)
}


case class CampaignBounce(
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
                           contactPersonConcatId: String,
                           contactPersonOhubId: Option[String],
                           bounceDate: Timestamp,
                           failureType: String,
                           failureReason: String,
                           isControlGroupMember: Boolean,
                           isProofGroupMember: Boolean,
                           operatorConcatId: Option[String],
                           operatorOhubId: Option[String],

                           // other fields
                           additionalFields: Map[String, String],
                           ingestionErrors: Map[String, IngestionError]
                         ) extends DomainEntity {
  override def getCompanion: DomainEntityCompanion = CampaignBounce
}
