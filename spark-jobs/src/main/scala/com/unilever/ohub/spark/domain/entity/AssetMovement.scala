package com.unilever.ohub.spark.domain.entity

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.DomainEntity.IngestionError
import com.unilever.ohub.spark.domain.{DomainEntity, DomainEntityCompanion}
import com.unilever.ohub.spark.export.ExportOutboundWriter
import com.unilever.ohub.spark.export.azuredw.AzureDWWriter
import com.unilever.ohub.spark.export.businessdatalake.{AzureDLWriter,AssetMovementDLWriter}
import com.unilever.ohub.spark.export.domain.DomainExportWriter

object AssetMovementDomainExportWriter extends DomainExportWriter[AssetMovement]

object AssetMovement extends DomainEntityCompanion[AssetMovement] {
  override val auroraFolderLocation = None
  override val engineFolderName: String = "assetmovements"
  override val domainExportWriter: Option[DomainExportWriter[AssetMovement]] = None
  override val acmExportWriter: Option[ExportOutboundWriter[AssetMovement]] = None
  override val dispatchExportWriter: Option[ExportOutboundWriter[AssetMovement]] = None
  override val azureDwWriter: Option[AzureDWWriter[AssetMovement]] = None
  override val auroraInboundWriter: Option[ExportOutboundWriter[AssetMovement]] = None
  override val dataLakeWriter: Option[AzureDLWriter[AssetMovement]] = Some(AssetMovementDLWriter)
}

case class AssetMovement(
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
                          operatorOhubId: Option[String],
                          assemblyDate: Option[Timestamp],
                          assetConcatId: Option[String],
                          createdBy: Option[String],
                          currency: Option[String],
                          deliveryDate: Option[Timestamp],
                          lastModifiedBy: Option[String],
                          name: Option[String],
                          comment: Option[String],
                          owner: Option[String],
                          quantityOfUnits: Option[Int],
                          `type`: Option[String],
                          returnDate: Option[Timestamp],
                          assetStatus: Option[String],

                          // other fields
                          additionalFields: Map[String, String],
                          ingestionErrors: Map[String, IngestionError]
                        ) extends DomainEntity {
  override def getCompanion: DomainEntityCompanion[AssetMovement] = AssetMovement
}
