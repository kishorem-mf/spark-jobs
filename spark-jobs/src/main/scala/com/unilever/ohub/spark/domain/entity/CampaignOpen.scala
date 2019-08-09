package com.unilever.ohub.spark.domain.entity

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.DomainEntity.IngestionError
import com.unilever.ohub.spark.domain.{DomainEntity, DomainEntityCompanion}
import com.unilever.ohub.spark.export.ExportOutboundWriter
import com.unilever.ohub.spark.export.azuredw.{AzureDWWriter, CampaignOpenDWWriter}
import com.unilever.ohub.spark.export.dispatch.CampaignOpenOutboundWriter
import com.unilever.ohub.spark.export.domain.DomainExportWriter

object CampaignOpenDomainExportWriter extends DomainExportWriter[CampaignOpen]

object CampaignOpen extends DomainEntityCompanion {
  val customerType = "CONTACTPERSON"
  override val engineFolderName = "campaignopens"
  override val domainExportWriter: Option[DomainExportWriter[CampaignOpen]] = Some(CampaignOpenDomainExportWriter)
  override val acmExportWriter: Option[ExportOutboundWriter[CampaignOpen]] = None
  override val dispatchExportWriter: Option[ExportOutboundWriter[CampaignOpen]] = Some(CampaignOpenOutboundWriter)
  override val azureDwWriter: Option[AzureDWWriter[CampaignOpen]] = Some(CampaignOpenDWWriter)
}

case class CampaignOpen(
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
                         trackingId: String,
                         campaignId: String,
                         campaignName: Option[String],
                         deliveryId: String,
                         deliveryName: String,
                         communicationChannel: String,
                         contactPersonConcatId: String,
                         contactPersonOhubId: Option[String],
                         operatorConcatId: Option[String],
                         operatorOhubId: Option[String],
                         openDate: Timestamp,
                         deliveryLogId: Option[String],

                         // other fields
                         additionalFields: Map[String, String],
                         ingestionErrors: Map[String, IngestionError]
                       ) extends DomainEntity {
  override def getCompanion: DomainEntityCompanion = CampaignOpen
}
