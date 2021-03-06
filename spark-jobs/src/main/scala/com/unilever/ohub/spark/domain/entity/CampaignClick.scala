package com.unilever.ohub.spark.domain.entity

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.DomainEntity.IngestionError
import com.unilever.ohub.spark.domain.{DomainEntity, DomainEntityCompanion}
import com.unilever.ohub.spark.export.ExportOutboundWriter
import com.unilever.ohub.spark.export.azuredw.{AzureDWWriter, CampaignClickDWWriter}
import com.unilever.ohub.spark.export.businessdatalake.{AzureDLWriter,CampaignClickDLWriter}
import com.unilever.ohub.spark.export.domain.DomainExportWriter

object CampaignClickDomainExportWriter extends DomainExportWriter[CampaignClick]

object CampaignClick extends DomainEntityCompanion[CampaignClick] {
  val customerType = "CONTACTPERSON"
  override val auroraFolderLocation = None
  override val engineFolderName = "campaignclicks"
  override val domainExportWriter: Option[DomainExportWriter[CampaignClick]] = Some(CampaignClickDomainExportWriter)
  override val acmExportWriter: Option[ExportOutboundWriter[CampaignClick]] = None
  override val dispatchExportWriter: Option[ExportOutboundWriter[CampaignClick]] = Some(com.unilever.ohub.spark.export.dispatch.CampaignClickOutboundWriter)
  override val azureDwWriter: Option[AzureDWWriter[CampaignClick]] = Some(CampaignClickDWWriter)
  override val auroraInboundWriter: Option[ExportOutboundWriter[CampaignClick]] = Some(com.unilever.ohub.spark.datalake.CampaignClickOutboundWriter)
  override val dataLakeWriter: Option[AzureDLWriter[CampaignClick]] = Some(CampaignClickDLWriter)
  override val ddlExportWriter: Option[ExportOutboundWriter[CampaignClick]] = None
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
                          contactPersonConcatId: Option[String],
                          contactPersonOhubId: String,
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
                        ) extends DomainEntity {
  override def getCompanion: DomainEntityCompanion[CampaignClick] = CampaignClick
}
