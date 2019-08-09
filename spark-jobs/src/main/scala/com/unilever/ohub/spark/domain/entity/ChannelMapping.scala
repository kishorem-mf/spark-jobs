package com.unilever.ohub.spark.domain.entity

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.DomainEntity.IngestionError
import com.unilever.ohub.spark.domain.{DomainEntity, DomainEntityCompanion}
import com.unilever.ohub.spark.export.domain.DomainExportWriter

object ChannelMapping extends DomainEntityCompanion {
  val customerType = "OPERATOR"
  override val engineFolderName: String = "channelmappings"
  override val domainExportWriter: Option[DomainExportWriter[ChannelMapping]] = None
}

object ChannelReference {
  val unknownChannelReferenceId = "-1"
}

case class ChannelReference(
                             channelReferenceId: String,
                             socialCommercial: Option[String],
                             strategicChannel: String,
                             globalChannel: String,
                             globalSubChannel: String
                           ) extends scala.Product

case class ChannelMapping(
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
                           originalChannel: String,
                           localChannel: String,
                           channelUsage: String,
                           channelReference: String,

                           // other fields
                           additionalFields: Map[String, String],
                           ingestionErrors: Map[String, IngestionError]
                         ) extends DomainEntity {
  override def getCompanion: DomainEntityCompanion = ChannelMapping

}
