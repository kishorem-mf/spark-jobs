package com.unilever.ohub.spark.domain.entity

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.DomainEntity.IngestionError
import com.unilever.ohub.spark.domain.{DomainEntity, DomainEntityCompanion}
import com.unilever.ohub.spark.export.domain.DomainExportWriter

object ActivityDomainExportWriter extends DomainExportWriter[Activity]

object Activity extends DomainEntityCompanion {
  override val engineFolderName: String = "activities"
  override val domainExportWriter: Option[DomainExportWriter[_]] = Some(ActivityDomainExportWriter)
}

case class Activity(
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
                     activityDate: Option[Timestamp],
                     name: Option[String],
                     details: Option[String],
                     actionType: Option[String],
                     contactPersonConcatId: Option[String],
                     contactPersonOhubId: Option[String],
                     operatorConcatId: Option[String],
                     operatorOhubId: Option[String],
                     activityId: Option[String],

                     // other fields
                     additionalFields: Map[String, String],
                     ingestionErrors: Map[String, IngestionError]
                   ) extends DomainEntity {
  override def getCompanion: DomainEntityCompanion = Activity
}
