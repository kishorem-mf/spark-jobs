package com.unilever.ohub.spark.domain.entity

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.DomainEntity.IngestionError
import com.unilever.ohub.spark.domain.{DomainEntity, DomainEntityCompanion}
import com.unilever.ohub.spark.export.ExportOutboundWriter
import com.unilever.ohub.spark.export.azuredw.{AzureDWWriter, CampaignSendDWWriter}
import com.unilever.ohub.spark.export.businessdatalake.{AzureDLWriter,CampaignSendDLWriter}
import com.unilever.ohub.spark.export.domain.DomainExportWriter

object CampaignSendDomainExportWriter extends DomainExportWriter[CampaignSend]

object CampaignSend extends DomainEntityCompanion[CampaignSend] {
  val customerType = "CONTACTPERSON"
  override val auroraFolderLocation = None
  override val engineFolderName = "campaignsends"
  override val domainExportWriter: Option[DomainExportWriter[CampaignSend]] = Some(CampaignSendDomainExportWriter)
  override val acmExportWriter: Option[ExportOutboundWriter[CampaignSend]] = None
  override val dispatchExportWriter: Option[ExportOutboundWriter[CampaignSend]] = Some(com.unilever.ohub.spark.export.dispatch.CampaignSendOutboundWriter)
  override val azureDwWriter: Option[AzureDWWriter[CampaignSend]] = Some(CampaignSendDWWriter)
  override val auroraInboundWriter: Option[ExportOutboundWriter[CampaignSend]] = Some(com.unilever.ohub.spark.datalake.CampaignSendOutboundWriter)
  override val dataLakeWriter: Option[AzureDLWriter[CampaignSend]] = Some(CampaignSendDLWriter)
  override val ddlExportWriter: Option[ExportOutboundWriter[CampaignSend]] = None
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
                         contactPersonConcatId: Option[String],
                         contactPersonOhubId: String,
                         waveName: String,

                         // other fields
                         additionalFields: Map[String, String],
                         ingestionErrors: Map[String, IngestionError]
                       ) extends DomainEntity {
  override def getCompanion: DomainEntityCompanion[CampaignSend] = CampaignSend
}
