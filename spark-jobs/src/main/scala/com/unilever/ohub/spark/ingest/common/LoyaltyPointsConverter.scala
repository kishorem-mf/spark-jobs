package com.unilever.ohub.spark.ingest.common

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.entity.LoyaltyPoints
import com.unilever.ohub.spark.ingest.CustomParsers.{parseDateTimeUnsafe, toBigDecimal, toTimestamp}
import com.unilever.ohub.spark.ingest.{ActivityEmptyParquetWriter, DomainTransformer}
import org.apache.spark.sql.Row

object LoyaltyPointsConverter extends CommonDomainGateKeeper[LoyaltyPoints] with ActivityEmptyParquetWriter {
  override protected def toDomainEntity: DomainTransformer ⇒ Row ⇒ LoyaltyPoints = { transformer ⇒
    row ⇒
      import transformer._
      implicit val source: Row = row

      val ohubCreated = new Timestamp(System.currentTimeMillis())

      LoyaltyPoints(
        id = mandatory("id"),
        creationTimestamp = mandatory("creationTimestamp", toTimestamp),
        concatId = mandatory("concatId"),
        countryCode = mandatory("countryCode"),
        customerType = mandatory("customerType"),
        dateCreated = optional("dateCreated", parseDateTimeUnsafe()),
        dateUpdated = optional("dateUpdated", parseDateTimeUnsafe()),
        isActive = true,
        isGoldenRecord = true,
        sourceEntityId = mandatory("sourceEntityId"),
        sourceName = mandatory("sourceName"),
        ohubId = Option.empty,
        ohubCreated = ohubCreated,
        ohubUpdated = ohubCreated,

        // specific fields
        totalEarned = optional("totalEarned", toBigDecimal),
        totalSpent = optional("totalSpent", toBigDecimal),
        totalActual = optional("totalActual", toBigDecimal),
        rewardGoal = optional("rewardGoal", toBigDecimal),
        contactPersonConcatId = optional("contactPersonConcatId"),
        contactPersonOhubId = Option.empty,
        operatorConcatId = optional("operatorConcatId"),
        operatorOhubId = Option.empty,
        rewardName = optional("rewardName"),
        rewardImageUrl = optional("rewardImageUrl"),
        rewardLandingPageUrl = optional("rewardLandingPageUrl"),
        rewardEanCode = optional("rewardEanCode"),
        additionalFields = additionalFields,
        ingestionErrors = errors
      )
  }
}
