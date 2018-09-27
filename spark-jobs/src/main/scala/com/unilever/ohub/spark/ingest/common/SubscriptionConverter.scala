package com.unilever.ohub.spark.ingest.common

import com.unilever.ohub.spark.domain.entity.Subscription
import com.unilever.ohub.spark.ingest.CustomParsers._
import com.unilever.ohub.spark.ingest.{ DomainTransformer, SubscriptionEmptyParquetWriter }
import org.apache.spark.sql.Row

object SubscriptionConverter extends CommonDomainGateKeeper[Subscription] with SubscriptionEmptyParquetWriter {

  override def toDomainEntity: DomainTransformer ⇒ Row ⇒ Subscription = { transformer ⇒ row ⇒
    import transformer._
    implicit val source: Row = row

    val countryCode = mandatoryValue("countryCode", "countryCode")(row)
    val concatIdRef: String = createConcatId("countryCode", "sourceName", "sourceEntityId")
    val ohubCreated = currentTimestamp()
    val contactPersonConcatRefId: String = createConcatId("countryCode", "sourceName", "contactPersonRefId")

    // format: OFF

    Subscription(
      // fieldName                  mandatory                   sourceFieldName             targetFieldName                 transformationFunction (unsafe)
      concatId                    = concatIdRef,
      countryCode                 = countryCode,
      customerType                = Subscription.customerType,
      dateCreated                 = optional(                   "dateCreated",                "dateCreated",                parseDateTimeStampUnsafe),
      dateUpdated                 = optional(                   "dateUpdated",                "dateUpdated",                parseDateTimeStampUnsafe),
      isActive                    = mandatory(                  "isActive",                   "isActive",                   parseBoolUnsafe),
      isGoldenRecord              = false,
      sourceEntityId              = mandatory(                  "sourceEntityId",             "sourceEntityId"),
      sourceName                  = mandatory(                  "sourceName",                 "sourceName"),
      ohubId                      = Option.empty,
      ohubCreated                 = ohubCreated,
      ohubUpdated                 = ohubCreated,
      contactPersonConcatId       = contactPersonConcatRefId,
      contactPersonOhubId         = Option.empty,
      communicationChannel        = optional(                   "communicationChannel",       "communicationChannel"),
      subscriptionType            = mandatory(                  "subscriptionType",           "subscriptionType"),
      hasSubscription             = mandatory(                  "hasSubscription",            "hasSubscription",            parseBoolUnsafe),
      subscriptionDate            = mandatory(                  "subscriptionDate",           "subscriptionDate",           parseDateTimeStampUnsafe),
      hasConfirmedSubscription    = optional(                   "hasConfirmedSubscription",   "hasConfirmedSubscription",   parseBoolUnsafe),
      confirmedSubscriptionDate   = optional(                   "confirmedSubscriptionDate",  "confirmedSubscriptionDate",  parseDateTimeStampUnsafe),
      additionalFields            = additionalFields,
      ingestionErrors             = errors
    )

    // format: ON
  }
}
