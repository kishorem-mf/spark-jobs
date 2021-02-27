package com.unilever.ohub.spark.export.ddl

import com.unilever.ohub.spark.domain.entity.Subscription
import com.unilever.ohub.spark.export.ddl.model.DdlNewsletterSubscription
import com.unilever.ohub.spark.export.{BooleanToYNConverter, Converter, TypeConversionFunctions}

object NewsletterSubscriptionDdlConverter extends Converter[Subscription, DdlNewsletterSubscription] with TypeConversionFunctions {

  override def convert(implicit nl: Subscription, explain: Boolean = false): DdlNewsletterSubscription = {
    DdlNewsletterSubscription(
      newsletterNumber = Option.empty,
      id = getValue("sourceEntityId"),
      contactSAPID = getValue("contactPersonOhubId"),
      createdBy = Option.empty,
      currency = Option.empty,
      deliveryMethod = getValue("communicationChannel"),
      includePricing = Option.empty,
      language = Option.empty,
      newsletterId = getValue("concatId"),
      newsletterName = Option.empty,
      owner = Option.empty,
      quantity = Option.empty,
      recordType = Option.empty,
      status = Option.empty,
      statusOptOut = Option.empty,
      subscribed = getValue("hasSubscription", BooleanToYNConverter),
      subscriptionConfirmed = getValue("hasConfirmedSubscription", BooleanToYNConverter),
      subscriptionConfirmedDate = getValue("confirmedSubscriptionDate"),
      subscriptionDate = getValue("subscriptionDate"),
      subscriptionType = getValue("subscriptionType"),
      unsubscriptionConfirmedDate = getValue("confirmedSubscriptionDate"),
      unsubscriptionDate = getValue("subscriptionDate"),
      comment = Option.empty
    )
  }
}
