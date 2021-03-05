package com.unilever.ohub.spark.domain.entity

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.DomainEntity.IngestionError
import com.unilever.ohub.spark.domain.{DomainEntity, DomainEntityCompanion}
import com.unilever.ohub.spark.export.ExportOutboundWriter
import com.unilever.ohub.spark.export.azuredw.{AzureDWWriter, CampaignDWWriter}
import com.unilever.ohub.spark.export.businessdatalake.{AzureDLWriter,CampaignDLWriter}
import com.unilever.ohub.spark.export.domain.DomainExportWriter

object CampaignDomainExportWriter extends DomainExportWriter[Campaign]


object Campaign extends DomainEntityCompanion[Campaign] {
  val customerType = "CONTACTPERSON"
  override val auroraFolderLocation = None
  override val engineFolderName: String = "campaigns"
  override val domainExportWriter: Option[DomainExportWriter[Campaign]] = Some(CampaignDomainExportWriter)
  override val acmExportWriter: Option[ExportOutboundWriter[Campaign]] = None
  override val dispatchExportWriter: Option[ExportOutboundWriter[Campaign]] = Some(com.unilever.ohub.spark.export.dispatch.CampaignOutboundWriter)
  override val azureDwWriter: Option[AzureDWWriter[Campaign]] = Some(CampaignDWWriter)
  override val auroraInboundWriter: Option[ExportOutboundWriter[Campaign]] = Some(com.unilever.ohub.spark.datalake.CampaignOutboundWriter)
  override val dataLakeWriter: Option[AzureDLWriter[Campaign]] = Some(CampaignDLWriter)
  ddlExportWriter: Option[ExportOutboundWriter[Campaign]] = None
}

case class Campaign(
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
                     contactPersonConcatId: Option[String],
                     contactPersonOhubId: String,
                     campaignId: String,
                     campaignName: Option[String],
                     deliveryId: String,
                     deliveryName: String,
                     campaignSpecification: Option[String],
                     campaignWaveStartDate: Timestamp,
                     campaignWaveEndDate: Timestamp,
                     campaignWaveStatus: String,

                     // other fields
                     additionalFields: Map[String, String],
                     ingestionErrors: Map[String, IngestionError]
                   ) extends DomainEntity {
  override def getCompanion: DomainEntityCompanion[Campaign] = Campaign
}
