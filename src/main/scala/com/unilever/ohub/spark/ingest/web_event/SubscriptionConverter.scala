package com.unilever.ohub.spark.ingest.web_event

import com.unilever.ohub.spark.domain.entity.Subscription
import com.unilever.ohub.spark.ingest.{ DomainTransformer, SubscriptionEmptyParquetWriter }
import org.apache.spark.sql.Row
import com.unilever.ohub.spark.domain.DomainEntity
import cats.syntax.option._

/**
 * Placeholder object for Subscription
 */
object SubscriptionConverter extends WebEventDomainGateKeeper[Subscription] with SubscriptionEmptyParquetWriter {
  override protected def toDomainEntity: DomainTransformer ⇒ Row ⇒ Subscription = { transformer ⇒ implicit row ⇒
    import transformer._

    val countryCode: String = mandatoryValue("countryCode", "countryCode")
    val sourceEntityId: String = mandatoryValue("sourceId", "sourceEntityId")
    val concatId: String = DomainEntity.createConcatIdFromValues(countryCode, SourceName, sourceEntityId)
    val ohubCreated = currentTimestamp()

      // format: OFF

      Subscription(
        concatId = concatId,
        countryCode = countryCode,
        customerType = Subscription.customerType,
        dateCreated = ohubCreated,
        dateUpdated = ohubCreated,
        isActive = false,
        isGoldenRecord = false,
        sourceEntityId = sourceEntityId,
        sourceName = SourceName,
        ohubId = none,
        ohubCreated = ohubCreated,
        ohubUpdated = ohubCreated,
        additionalFields = additionalFields,
        ingestionErrors = errors
      )

    // format: ON
  }
}
