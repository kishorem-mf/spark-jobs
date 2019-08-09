package com.unilever.ohub.spark.domain.entity

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.DomainEntity.IngestionError
import com.unilever.ohub.spark.domain.{DomainEntity, DomainEntityCompanion}
import com.unilever.ohub.spark.export.ExportOutboundWriter
import com.unilever.ohub.spark.export.azuredw.{AzureDWWriter, QuestionDWWriter}
import com.unilever.ohub.spark.export.domain.DomainExportWriter

object QuestionDomainExportWriter extends DomainExportWriter[Question]

object Question extends DomainEntityCompanion {
  override val engineFolderName: String = "questions"
  override val domainExportWriter: Option[DomainExportWriter[Question]] = Some(QuestionDomainExportWriter)
  override val acmExportWriter: Option[ExportOutboundWriter[Question]] = None
  override val dispatchExportWriter: Option[ExportOutboundWriter[Question]] = None
  override val azureDwWriter: Option[AzureDWWriter[Question]] = Some(QuestionDWWriter)
}

case class Question(
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
                     activityConcatId: String,
                     question: Option[String],

                     // other fields
                     additionalFields: Map[String, String],
                     ingestionErrors: Map[String, IngestionError]
                   ) extends DomainEntity {
  override def getCompanion: DomainEntityCompanion = Question
}

