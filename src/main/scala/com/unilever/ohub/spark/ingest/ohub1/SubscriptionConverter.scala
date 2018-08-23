package com.unilever.ohub.spark.ingest.ohub1

import java.sql.Timestamp

import cats.syntax.option.none
import com.unilever.ohub.spark.domain.DomainEntity.IngestionError
import com.unilever.ohub.spark.domain.entity.Subscription
import com.unilever.ohub.spark.ingest.CustomParsers._
import com.unilever.ohub.spark.ingest.web_event_interface.SubscriptionConverter.SourceName
import com.unilever.ohub.spark.ingest.{ DomainTransformer, SubscriptionEmptyParquetWriter }
import org.apache.spark.sql.Row

object SubscriptionConverter extends EmakinaDomainGateKeeper[Subscription] with SubscriptionEmptyParquetWriter {

  override def toDomainEntity: DomainTransformer ⇒ Row ⇒ Subscription = { transformer ⇒ row ⇒
    import transformer._
    implicit val source: Row = row

    val countryCode = mandatoryValue("COUNTRY_CODE", "countryCode")(row)
    val concatIdRef: String = createConcatId("COUNTRY_CODE", "SOURCE", "REF_SUBSCRIPTION_ID")
    val ohubCreated = currentTimestamp()
    val contactPersonConcatRefId: String = createConcatId("COUNTRY_CODE", "SOURCE", "REF_CONTACT_PERSON_ID")

    Subscription(
      // fieldName                  mandatory   sourceFieldName             targetFieldName                 transformationFunction (unsafe)
      concatId = concatIdRef,
      countryCode = countryCode,
      customerType = Subscription.customerType,
      dateCreated = optional("DATE_CREATED", "dateCreated", parseDateTimeStampUnsafe),
      dateUpdated = optional("DATE_MODIFIED", "dateUpdated", parseDateTimeStampUnsafe),
      isActive = mandatory("STATUS", "isActive", parseBoolUnsafe),
      isGoldenRecord = false,
      sourceEntityId = mandatory("REF_SUBSCRIPTION_ID", "sourceEntityId"),
      sourceName = mandatory("SOURCE", "sourceName"),
      ohubId = Option.empty,
      ohubCreated = ohubCreated,
      ohubUpdated = ohubCreated,
      contactPersonConcatId = contactPersonConcatRefId,
      communicationChannel = Some("Some channel"),
      subscriptionType = mandatory("SUBSCRIPTION_TYPE", "subscriptionType"),
      hasSubscription = mandatory("SUBSCRIBED", "hasSubscription", parseBoolUnsafe),
      subscriptionDate = mandatory("SUBSCRIPTION_DATE", "subscriptionDate", parseDateTimeStampUnsafe),
      hasConfirmedSubscription = optional("SUBSCRIPTION_CONFIRMED", "hasConfirmedSubscription", parseBoolUnsafe),
      ConfirmedSubscriptionDate = optional("SUBSCRIPTION_CONFIRMED_DATE", "confirmedSubscriptionDate", parseDateTimeStampUnsafe),
      additionalFields = additionalFields,
      ingestionErrors = errors
    )
  }
}
