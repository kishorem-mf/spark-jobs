package com.unilever.ohub.spark.domain

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.DomainEntity.IngestionError
import com.unilever.ohub.spark.export.ExportOutboundWriter
import com.unilever.ohub.spark.export.azuredw.AzureDWWriter
import com.unilever.ohub.spark.export.domain.DomainExportWriter

object DomainEntity {
  case class IngestionError(originalColumnName: String, inputValue: Option[String], exceptionMessage: String)
}

// marker trait for all domain entities (press ctrl + h in IntelliJ to see all)
trait DomainEntity extends Product {
  // mandatory fields
  val id: String
  val creationTimestamp: Timestamp
  val concatId: String
  val countryCode: String
  val customerType: String
  val sourceEntityId: String
  val sourceName: String
  val isActive: Boolean
  val ohubCreated: Timestamp
  val ohubUpdated: Timestamp

  // optional fields
  val dateCreated: Option[Timestamp]
  val dateUpdated: Option[Timestamp]

  // used for grouping and marking the golden record within the group
  val ohubId: Option[String]
  val isGoldenRecord: Boolean

  // aggregated fields
  val additionalFields: Map[String, String]
  val ingestionErrors: Map[String, IngestionError]

  // ENABLE IF THE ENTITY SHOULDN'T BE CREATED WHEN INGESTION ERRORS ARE PRESENT
  // assert(ingestionErrors.isEmpty, s"can't create domain entity due to '${ingestionErrors.size}' ingestion error(s): '${ingestionErrors.keySet.toSeq.sorted.mkString(",")}'")
  def getCompanion : DomainEntityCompanion[_ <: DomainEntity]
}

object DomainEntityCompanion {
  val defaultExcludedFieldsForCsvExport = Seq("additionalFields", "ingestionErrors")
  val defaultExcludedFieldsForParquetExport = Seq.empty[String]
}

trait DomainEntityCompanion[DomainEntityType <: DomainEntity] {
  val engineFolderName: String
  val excludedFieldsForCsvExport: Seq[String] = DomainEntityCompanion.defaultExcludedFieldsForCsvExport
  val defaultExcludedFieldsForParquetExport = Seq.empty[String]
  val domainExportWriter: Option[DomainExportWriter[DomainEntityType]]
  val acmExportWriter: Option[ExportOutboundWriter[DomainEntityType]]
  val dispatchExportWriter: Option[ExportOutboundWriter[DomainEntityType]]
  val azureDwWriter: Option[AzureDWWriter[DomainEntityType]]
}
