package com.unilever.ohub.spark.domain.entity

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.DomainEntity.IngestionError
import com.unilever.ohub.spark.domain.{DomainEntity, DomainEntityCompanion}
import com.unilever.ohub.spark.export.domain.DomainExportWriter

object SubscriptionDomainExportWriter extends DomainExportWriter[Subscription]

object Subscription extends DomainEntityCompanion {
  val customerType = "SUBSCRIPTION"
  override val engineFolderName: String = "subscriptions"
  override val domainExportWriter: Option[DomainExportWriter[Subscription]] = Some(SubscriptionDomainExportWriter)
}

case class Subscription(
                         // generic fields
                         id: String,
                         creationTimestamp: Timestamp,
                         concatId: String,
                         countryCode: String,
                         customerType: String,
                         dateCreated: Option[Timestamp],
                         dateUpdated: Option[Timestamp],
                         isActive: Boolean,
                         isGoldenRecord: Boolean,
                         sourceEntityId: String,
                         sourceName: String,
                         ohubId: Option[String],
                         ohubCreated: Timestamp,
                         ohubUpdated: Timestamp,
                         // specific fields
                         contactPersonConcatId: String,
                         contactPersonOhubId: Option[String],
                         communicationChannel: Option[String],
                         subscriptionType: String,
                         hasSubscription: Boolean,
                         subscriptionDate: Option[Timestamp],
                         hasConfirmedSubscription: Option[Boolean],
                         confirmedSubscriptionDate: Option[Timestamp],
                         fairKitchensSignUpType: Option[String],
                         // other fields
                         additionalFields: Map[String, String],
                         ingestionErrors: Map[String, IngestionError]
                       ) extends DomainEntity {
  override def getCompanion: DomainEntityCompanion = Subscription
}
