package com.unilever.ohub.spark.ingest.common

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.entity.Subscription
import com.unilever.ohub.spark.ingest.CustomParsers._
import com.unilever.ohub.spark.ingest.{DomainTransformer, SubscriptionEmptyParquetWriter}
import org.apache.spark.sql.expressions.WindowSpec
import org.apache.spark.sql.{Row, SparkSession}

object SubscriptionConverter extends CommonDomainGateKeeper[Subscription] with SubscriptionEmptyParquetWriter {

  override def toDomainEntity: DomainTransformer ⇒ Row ⇒ Subscription = { transformer ⇒
    row ⇒
      import transformer._
      implicit val source: Row = row

      val ohubCreated = new Timestamp(System.currentTimeMillis())

      Subscription(
        id = mandatory("id"),
        creationTimestamp = mandatory("creationTimestamp", toTimestamp),
        concatId = mandatory("concatId"),
        countryCode = mandatory("countryCode"),
        customerType = Subscription.customerType,
        dateCreated = optional("dateCreated", parseDateTimeUnsafe()),
        dateUpdated = optional("dateUpdated", parseDateTimeUnsafe()),
        isActive = mandatory("isActive", toBoolean),
        isGoldenRecord = false,
        sourceEntityId = mandatory("sourceEntityId"),
        sourceName = mandatory("sourceName"),
        ohubId = Option.empty,
        ohubCreated = ohubCreated,
        ohubUpdated = ohubCreated,
        contactPersonConcatId = mandatory("contactPersonConcatId"),
        contactPersonOhubId = Option.empty,
        communicationChannel = optional("communicationChannel"),
        subscriptionType = mandatory("subscriptionType"),
        hasSubscription = mandatory("hasSubscription", toBoolean),
        subscriptionDate = optional("subscriptionDate", parseDateTimeUnsafe()),
        hasConfirmedSubscription = optional("hasConfirmedSubscription", toBoolean),
        confirmedSubscriptionDate = optional("confirmedSubscriptionDate", parseDateTimeUnsafe()),
        fairKitchensSignUpType = optional("fairKitchensSignUpType"),
        //BDL FIELDS
        newsletterNumber = optional("newsletterNumber"),
        createdBy = optional("createdBy"),
        currency = optional("currency"),
        hasPricingInfo = optional("hasPricingInfo", toBoolean),
        language = optional("language"),
        name = optional("name"),
        owner = optional("owner"),
        numberOfTimesSent = optional("numberOfTimesSent", toInt),
        localOrGlobalSendOut = optional("localOrGlobalSendOut"),
        comment = optional("comment"),
        additionalFields = additionalFields,
        ingestionErrors = errors
      )
  }

  /**
    * @inheritdoc
    * @param spark      sparksession, user for implicits import
    * @param windowSpec windowspec that is sorted by this function
    * @return a sorted dedplicationWindowSpec
    */
  override def sortDeduplicationWindowSpec(spark: SparkSession, windowSpec: WindowSpec): WindowSpec = {
    import spark.implicits._

    windowSpec.orderBy(
      $"subscriptionDate".desc_nulls_last,
      $"confirmedSubscriptionDate".desc_nulls_last,
      $"dateUpdated".desc_nulls_last,
      $"dateCreated".desc_nulls_last,
      $"ohubUpdated".desc_nulls_last)
  }
}
