package com.unilever.ohub.spark.ingest.common

import com.unilever.ohub.spark.domain.entity.LoyaltyPoints
import com.unilever.ohub.spark.ingest.CustomParsers.{ parseDateTimeUnsafe, toBigDecimal, toTimestamp }
import com.unilever.ohub.spark.ingest.{ ActivityEmptyParquetWriter, DomainTransformer }
import org.apache.spark.sql.Row

object LoyaltyPointsConverter extends CommonDomainGateKeeper[LoyaltyPoints] with ActivityEmptyParquetWriter {
  override protected def toDomainEntity: DomainTransformer ⇒ Row ⇒ LoyaltyPoints = { transformer ⇒ row ⇒
    import transformer._
    implicit val source: Row = row

    val ohubCreated = currentTimestamp()

    LoyaltyPoints(
      //fieldName                     mandatory                                    sourceFieldName                               targetFieldName               transformationFunction (unsafe)
      id                            = mandatory("id", "id"),
      creationTimestamp             = mandatory("creationTimestamp", "creationTimestamp", toTimestamp),
      concatId                      = mandatory("concatId", "concatId"),
      countryCode                   = mandatory("countryCode", "countryCode"),
      customerType                  = mandatory("customerType", "customerType"),
      dateCreated                   = optional("dateCreated", "dateCreated", parseDateTimeUnsafe()),
      dateUpdated                   = optional("dateUpdated", "dateUpdated", parseDateTimeUnsafe()),
      isActive                      = true,
      isGoldenRecord                = true,
      sourceEntityId                = mandatory("sourceEntityId", "sourceEntityId"),
      sourceName                    = mandatory("sourceName", "sourceName"),
      ohubId                        = Option.empty,
      ohubCreated                   = ohubCreated,
      ohubUpdated                   = ohubCreated,

      // specific fields
      totalLoyaltyPointsEarned      = optional("totalLoyaltyPointsEarned", "totalLoyaltyPointsEarned", toBigDecimal),
      totalLoyaltyPointsSpent       = optional("totalLoyaltyPointsSpent", "totalLoyaltyPointsSpent", toBigDecimal),
      totalLoyaltyPointsActual      = optional("totalLoyaltyPointsActual", "totalLoyaltyPointsActual", toBigDecimal),
      loyaltyRewardGoal             = optional("loyaltyRewardGoal", "loyaltyRewardGoal", toBigDecimal),
      contactPersonRefId            = optional("contactPersonRefId", "contactPersonRefId"),
      contactPersonConcatId         = optional("contactPersonConcatId", "contactPersonConcatId"),
      contactPersonOhubId           = Option.empty,
      operatorRefId                 = optional("operatorRefId", "operatorRefId"),
      operatorConcatId              = optional("operatorConcatId", "operatorConcatId"),
      operatorOhubId                = Option.empty,

      additionalFields              = additionalFields,
      ingestionErrors               = errors
    )
  }
}
