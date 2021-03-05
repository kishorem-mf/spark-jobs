package com.unilever.ohub.spark.domain.entity


import java.sql.Date

import com.unilever.ohub.spark.domain.{DomainEntity, DomainEntityCompanion}
import com.unilever.ohub.spark.export.ExportOutboundWriter
import com.unilever.ohub.spark.export.azuredw.{AzureDWWriter, OperatorChangeLogDWWriter}
import com.unilever.ohub.spark.export.businessdatalake.{AzureDLWriter, OperatorChangeLogDLWriter}
import com.unilever.ohub.spark.export.domain.DomainExportWriter

object OperatorChangeLogDomainExportWriter extends DomainExportWriter[OperatorChangeLog]

object OperatorChangeLog extends DomainEntityCompanion[OperatorChangeLog]{

  override val engineFolderName: String = "operators_change_log"
  override val auroraFolderLocation = Some("Shared")
  override val domainExportWriter: Option[DomainExportWriter[OperatorChangeLog]] = Some(OperatorChangeLogDomainExportWriter)
  override val acmExportWriter: Option[ExportOutboundWriter[OperatorChangeLog]] = None
  override val dispatchExportWriter: Option[ExportOutboundWriter[OperatorChangeLog]] = None
  override val azureDwWriter: Option[AzureDWWriter[OperatorChangeLog]] = Some(OperatorChangeLogDWWriter)
  override val excludedFieldsForCsvExport: Seq[String] = DomainEntityCompanion.defaultExcludedFieldsForCsvExport ++
    Seq("id", "creationTimestamp", "countryCode", "customerType", "sourceEntityId", "sourceName", "isActive", "ohubCreated", "ohubUpdated",
    "dateCreated", "dateUpdated", "isGoldenRecord")
  override val defaultExcludedFieldsForParquetExport: Seq[String] = DomainEntityCompanion.defaultExcludedFieldsForParquetExport ++
    Seq("id", "creationTimestamp", "countryCode", "customerType", "sourceEntityId", "sourceName", "isActive", "ohubCreated", "ohubUpdated",
      "dateCreated", "dateUpdated", "isGoldenRecord")
  override val auroraInboundWriter: Option[ExportOutboundWriter[OperatorChangeLog]] = None
  override val dataLakeWriter: Option[AzureDLWriter[OperatorChangeLog]] = Some(OperatorChangeLogDLWriter)
  override val ddlExportWriter: Option[ExportOutboundWriter[OperatorChangeLog]] = None
}


//case class OperatorChangeLog(
//                       id: String,
//                       creationTimestamp: Timestamp,
//                       concatId: String,
//                       countryCode: String,
//                       customerType: String,
//                       sourceEntityId: String,
//                       sourceName: String,
//                       isActive: Boolean,
//                       ohubCreated: Timestamp,
//                       ohubUpdated: Timestamp,
//
//                       // optional fields
//                       dateCreated: Option[Timestamp],
//                       dateUpdated: Option[Timestamp],
//                       // used for grouping and marking the golden record within the group
//                       ohubId: Option[String],
//                       isGoldenRecord: Boolean,
//
//                       //required fields
//                       fromDate: Option[Date],
//                       toDate: Option[Date],
//
//                       // other fields
//                       additionalFields: Map[String, String],
//                       ingestionErrors: Map[String, IngestionError]
//
//                     ) extends DomainEntity {
//  override def getCompanion: DomainEntityCompanion[OperatorChangeLog] = OperatorChangeLog
//
//  }
abstract case class OperatorChangeLog(
                              ohubId: Option[String],
                              concatId: String,
                              fromDate: Option[Date],
                              toDate: Option[Date]

                            ) extends DomainEntity {
  override def getCompanion: DomainEntityCompanion[OperatorChangeLog] = OperatorChangeLog

}
