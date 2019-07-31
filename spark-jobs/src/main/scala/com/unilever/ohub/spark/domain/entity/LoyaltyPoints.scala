package com.unilever.ohub.spark.domain.entity

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.DomainEntity.IngestionError
import com.unilever.ohub.spark.domain.{DomainEntity, DomainEntityCompanion}

object LoyaltyPoints extends DomainEntityCompanion {
  override val engineFolderName: String = "loyaltypoints"
}

case class LoyaltyPoints(
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
                          totalEarned: Option[BigDecimal],
                          totalSpent: Option[BigDecimal],
                          totalActual: Option[BigDecimal],
                          rewardGoal: Option[BigDecimal],
                          contactPersonConcatId: Option[String],
                          contactPersonOhubId: Option[String],
                          operatorConcatId: Option[String],
                          operatorOhubId: Option[String],
                          rewardName: Option[String],
                          rewardImageUrl: Option[String],
                          rewardLandingPageUrl: Option[String],
                          rewardEanCode: Option[String],

                          // other fields
                          additionalFields: Map[String, String],
                          ingestionErrors: Map[String, IngestionError]
                        ) extends DomainEntity {}
