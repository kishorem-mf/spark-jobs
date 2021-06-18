package com.unilever.ohub.spark.domain.entity

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.DomainEntity.IngestionError
import com.unilever.ohub.spark.domain.{DomainEntity, DomainEntityCompanion}
import com.unilever.ohub.spark.export.ExportOutboundWriter
import com.unilever.ohub.spark.export.azuredw.AzureDWWriter
import com.unilever.ohub.spark.export.businessdatalake.AzureDLWriter
import com.unilever.ohub.spark.export.domain.DomainExportWriter

object EntityRelationshipsDomainExportWriter extends DomainExportWriter[EntityRelationships]

object EntityRelationships extends DomainEntityCompanion[EntityRelationships] {
  override val auroraFolderLocation = None
  override val engineFolderName: String = "entityrelationships"
  override val domainExportWriter: Option[DomainExportWriter[EntityRelationships]] = None
  override val acmExportWriter: Option[ExportOutboundWriter[EntityRelationships]] = None
  override val dispatchExportWriter: Option[ExportOutboundWriter[EntityRelationships]] = None
  override val azureDwWriter: Option[AzureDWWriter[EntityRelationships]] = None
  override val ddlExportWriter: Option[ExportOutboundWriter[EntityRelationships]] = None
  override val auroraInboundWriter: Option[ExportOutboundWriter[EntityRelationships]] = None
  override val dataLakeWriter: Option[AzureDLWriter[EntityRelationships]] = None
}

case class EntityRelationships(
                                // generic fields
                                // mandatory fields
                                id: String,
                                creationTimestamp: Timestamp,
                                countryCode: String,
                                sourceName: String,
                                sourceEntityId: String,
                                concatId: String,
                                relationshipName: Option[String],
                                relationshipType: String,
                                fromEntityType: String,
                                fromEntitySourceName: String,
                                fromSourceEntityId: String,
                                fromConcatId: String,
                                fromEntityOhubId: Option[String],
                                fromEntityName: Option[String],
                                toEntityType: String,
                                toEntitySourceName: String,
                                toSourceEntityId: String,
                                toConcatId: String,
                                toEntityOhubId: Option[String],
                                toEntityName: Option[String],
                                isActive: Boolean,
                                validFrom: Option[Timestamp],
                                validTo: Option[Timestamp],
                                dateCreated: Option[Timestamp],
                                dateUpdated: Option[Timestamp],

                                customerType: String,
                                ohubCreated: Timestamp,
                                ohubUpdated: Timestamp,

                                // used for grouping and marking the golden record within the group
                                ohubId: Option[String],
                                isGoldenRecord: Boolean,
                                // other fields
                                additionalFields: Map[String, String],
                                ingestionErrors: Map[String, IngestionError]
                              ) extends DomainEntity {
  override def getCompanion: DomainEntityCompanion[EntityRelationships] = EntityRelationships
}
