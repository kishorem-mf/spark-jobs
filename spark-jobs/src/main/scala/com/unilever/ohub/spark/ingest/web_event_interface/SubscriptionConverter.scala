package com.unilever.ohub.spark.ingest.web_event_interface

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.entity.Subscription
import com.unilever.ohub.spark.ingest.{ DomainTransformer, SubscriptionEmptyParquetWriter }
import org.apache.spark.sql.Row
import com.unilever.ohub.spark.domain.DomainEntity
import cats.syntax.option._
import com.unilever.ohub.spark.ingest.CustomParsers.{ parseBoolUnsafe, parseDateTimeStampUnsafe }

/**
 * Placeholder object for Subscription
 */
object SubscriptionConverter extends WebEventDomainGateKeeper[Subscription] with SubscriptionEmptyParquetWriter {
  override protected def toDomainEntity: DomainTransformer ⇒ Row ⇒ Subscription = { transformer ⇒ implicit row ⇒
    import transformer._

    val countryCode: String = mandatoryValue("countryCode", "countryCode")
    val sourceEntityId: String = mandatoryValue("sourceId", "sourceEntityId")
    val concatId: String = DomainEntity.createConcatIdFromValues(countryCode, SourceName, sourceEntityId)
    val contactPersonRefId = mandatory("contactPersonRefId", "contactPersonConcatId")
    val contactPersonConcatId: String = DomainEntity.createConcatIdFromValues(countryCode, sourceEntityId, contactPersonRefId)
    val ohubCreated = currentTimestamp()

      // format: OFF

      Subscription(
        // fieldName                  mandatory   sourceFieldName             targetFieldName                 transformationFunction (unsafe)
        concatId                =     concatId,
        countryCode             =     countryCode,
        customerType            =     Subscription.customerType,
        dateCreated             =     None,
        dateUpdated             =     None,
        isActive                =     true,
        isGoldenRecord          =     true,
        sourceEntityId          =     sourceEntityId,
        sourceName              =     SourceName,
        ohubId                  =     none,// set in SubscriptionMerging
        ohubCreated             =     ohubCreated,
        ohubUpdated             =     ohubCreated,
        contactPersonConcatId   =     contactPersonConcatId,
        contactPersonOhubId   =     None, // set in SubscriptionMerging
        communicationChannel    =     optional(  "communicationChannel",     "communicationChannel"),
        subscriptionType        =     mandatory(  "subscriptionType" ,        "subscriptionType"),
        hasSubscription         =     mandatory(  "subscribed",               "hasSubscription",                    parseBoolUnsafe),
        subscriptionDate        =     mandatory(  "subscribedDate",           "subscriptionDate",                   parseDateTimeStampUnsafe),
        hasConfirmedSubscription =    optional(   "subscriptionConfirmed",    "hasConfirmedSubscription",           parseBoolUnsafe),
        confirmedSubscriptionDate =   optional(   "subscriptionConfirmedDate", "ConfirmedSubscriptionDate",         parseDateTimeStampUnsafe),
        additionalFields          =   additionalFields,
        ingestionErrors           =   errors
      )

    // format: ON
  }
}
