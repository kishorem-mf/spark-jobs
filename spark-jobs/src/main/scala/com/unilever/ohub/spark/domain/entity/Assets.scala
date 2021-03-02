package com.unilever.ohub.spark.domain.entity

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.DomainEntity.IngestionError
import com.unilever.ohub.spark.domain.{DomainEntity, DomainEntityCompanion}
import com.unilever.ohub.spark.export.ExportOutboundWriter
import com.unilever.ohub.spark.export.azuredw.{AssetDWWriter, AzureDWWriter}
import com.unilever.ohub.spark.export.businessdatalake.{AzureDLWriter,AssetDLWriter}
import com.unilever.ohub.spark.export.domain.DomainExportWriter

object AssetDomainExportWriter extends DomainExportWriter[Asset]

object Asset extends DomainEntityCompanion[Asset] {
  override val auroraFolderLocation = None
  override val engineFolderName: String = "assets"
  override val domainExportWriter: Option[DomainExportWriter[Asset]] = Some(AssetDomainExportWriter)
  override val acmExportWriter: Option[ExportOutboundWriter[Asset]] = None
  override val dispatchExportWriter: Option[ExportOutboundWriter[Asset]] = None
  override val azureDwWriter: Option[AzureDWWriter[Asset]] = Some(AssetDWWriter)
  override val auroraInboundWriter: Option[ExportOutboundWriter[Asset]] = None
  override val dataLakeWriter: Option[AzureDLWriter[Asset]] = Some(AssetDLWriter)
}

case class Asset(
                     // generic fields
                     // mandatory fields
                     id: String,
                     creationTimestamp: Timestamp,
                     concatId: String,
                     countryCode: String,
                     customerType: String,
                     sourceEntityId: String,
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
                     // specific fields
                     crmId: Option[String],
                     name: Option[String],
                     `type`: Option[String],
                     brandName: Option[String],
                     description: Option[String],
                     dimensions: Option[String],
                     numberOfTimesRepaired: Option[Int],
                     powerConsumption: Option[String],
                     numberOfCabinetBaskets: Option[Int],
                     numberOfCabinetFacings: Option[Int],
                     serialNumber: Option[String],
                     oohClassification: Option[String],
                     lifecyclePhase: Option[String],
                     capacityInLiters: Option[BigDecimal],
                     dateCreatedInUdl: Option[Timestamp],
                     leasedOrSold: Option[String],
                     crmTaskId: Option[String],
                    // other fields
                     additionalFields: Map[String, String],
                     ingestionErrors: Map[String, IngestionError]
                   ) extends DomainEntity {
  override def getCompanion: DomainEntityCompanion[Asset] = Asset
}
