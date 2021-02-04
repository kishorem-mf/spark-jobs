package com.unilever.ohub.spark.domain.entity

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.DomainEntity.IngestionError
import com.unilever.ohub.spark.domain.{DomainEntity, DomainEntityCompanion}
import com.unilever.ohub.spark.export.ExportOutboundWriter
import com.unilever.ohub.spark.export.azuredw.{AnswerDWWriter, AzureDWWriter}
import com.unilever.ohub.spark.export.domain.DomainExportWriter

object AnswerDomainExportWriter extends DomainExportWriter[Answer]

object Answer extends DomainEntityCompanion[Answer] {
  override val auroraFolderLocation = None
  override val engineFolderName: String = "answers"
  override val domainExportWriter: Option[DomainExportWriter[Answer]] = Some(AnswerDomainExportWriter)
  override val acmExportWriter: Option[ExportOutboundWriter[Answer]] = None
  override val dispatchExportWriter: Option[ExportOutboundWriter[Answer]] = None
  override val azureDwWriter: Option[AzureDWWriter[Answer]] = Some(AnswerDWWriter)
  override val auroraExportWriter: Option[ExportOutboundWriter[Answer]] = Some(com.unilever.ohub.spark.export.aurora.AnswerOutboundWriter)
  override val ddlExportWriter: Option[ExportOutboundWriter[Answer]] = None
}

case class Answer(
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
                   answer: Option[String],
                   questionConcatId: String,

                   // other fields
                   additionalFields: Map[String, String],
                   ingestionErrors: Map[String, IngestionError]
                 ) extends DomainEntity {
  override def getCompanion: DomainEntityCompanion[Answer] = Answer
}
