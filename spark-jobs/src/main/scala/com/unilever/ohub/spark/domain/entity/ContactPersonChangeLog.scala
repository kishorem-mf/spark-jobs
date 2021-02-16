package com.unilever.ohub.spark.domain.entity

import java.sql.{Date, Timestamp}

import com.unilever.ohub.spark.domain.{DomainEntity, DomainEntityCompanion}
import com.unilever.ohub.spark.export.ExportOutboundWriter
import com.unilever.ohub.spark.export.azuredw.{AzureDWWriter, ContactPersonChangeLogDWWriter}
import com.unilever.ohub.spark.export.businessdatalake.{AzureDLWriter,ContactPersonChangeLogDLWriter}
import com.unilever.ohub.spark.export.domain.DomainExportWriter

object ContactPersonChangeLogDomainExportWriter extends DomainExportWriter[ContactPersonChangeLog]

object ContactPersonChangeLog extends DomainEntityCompanion[ContactPersonChangeLog] {

  override val engineFolderName: String = "contactpersons_change_log"
  override val auroraFolderLocation = Some("Shared")
  override val domainExportWriter: Option[DomainExportWriter[ContactPersonChangeLog]] = Some(ContactPersonChangeLogDomainExportWriter)
  override val acmExportWriter: Option[ExportOutboundWriter[ContactPersonChangeLog]] = None
  override val dispatchExportWriter: Option[ExportOutboundWriter[ContactPersonChangeLog]] = None
  override val azureDwWriter: Option[AzureDWWriter[ContactPersonChangeLog]] = Some(ContactPersonChangeLogDWWriter)
  override val excludedFieldsForCsvExport: Seq[String] = DomainEntityCompanion.defaultExcludedFieldsForCsvExport ++
    Seq("id", "creationTimestamp", "countryCode", "customerType", "sourceEntityId", "sourceName", "isActive", "ohubCreated", "ohubUpdated",
      "dateCreated", "dateUpdated", "isGoldenRecord")
  override val defaultExcludedFieldsForParquetExport: Seq[String] = DomainEntityCompanion.defaultExcludedFieldsForParquetExport ++
    Seq("id", "creationTimestamp", "countryCode", "customerType", "sourceEntityId", "sourceName", "isActive", "ohubCreated", "ohubUpdated",
      "dateCreated", "dateUpdated", "isGoldenRecord")
  override val auroraInboundWriter: Option[ExportOutboundWriter[ContactPersonChangeLog]] = None
  override val dataLakeWriter: Option[AzureDLWriter[ContactPersonChangeLog]] = Some(ContactPersonChangeLogDLWriter)
}
//
//case class ContactPersonChangeLog(
//                              id: String,
//                              creationTimestamp: Timestamp,
//                              concatId: String,
//                              countryCode: String,
//                              customerType: String,
//                              sourceEntityId: String,
//                              sourceName: String,
//                              isActive: Boolean,
//                              ohubCreated: Timestamp,
//                              ohubUpdated: Timestamp,
//
//                              // optional fields
//                              dateCreated: Option[Timestamp],
//                              dateUpdated: Option[Timestamp],
//                              // used for grouping and marking the golden record within the group
//                              ohubId: Option[String],
//                              isGoldenRecord: Boolean,
//
//                              //required fields
//                              fromDate: Option[Date],
//                              toDate: Option[Date],
//
//                              // other fields
//                              additionalFields: Map[String, String],
//                              ingestionErrors: Map[String, IngestionError]
//
//                            ) extends DomainEntity {
//  override def getCompanion: DomainEntityCompanion[ContactPersonChangeLog] = ContactPersonChangeLog
//}

abstract case class ContactPersonChangeLog(
                                   ohubId: Option[String],
                                   concatId: String,

                                   fromDate: Option[Date],
                                   toDate: Option[Date]


                                 ) extends DomainEntity {
  override def getCompanion: DomainEntityCompanion[ContactPersonChangeLog] = ContactPersonChangeLog
}
