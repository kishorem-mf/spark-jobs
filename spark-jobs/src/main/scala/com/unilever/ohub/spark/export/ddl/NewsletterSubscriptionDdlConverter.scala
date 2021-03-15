package com.unilever.ohub.spark.export.ddl

import com.unilever.ohub.spark.domain.entity.Subscription
import com.unilever.ohub.spark.export.ddl.model.DdlNewsletterSubscription
import com.unilever.ohub.spark.export.{BooleanToYNConverter, Converter, TypeConversionFunctions}

object NewsletterSubscriptionDdlConverter extends Converter[Subscription, DdlNewsletterSubscription] with TypeConversionFunctions {

  override def convert(implicit nl: Subscription, explain: Boolean = false): DdlNewsletterSubscription = {
    DdlNewsletterSubscription(
      `Newsletter Number` = Option.empty,
      ID = getValue("sourceEntityId"),
      `Contact SAP ID` = getValue("contactPersonOhubId"),
      `Created By` = Option.empty,
      Currency = Option.empty,
      `Delivery Method` = getValue("communicationChannel"),
      `Include Pricing` = Option.empty,
      Language = Option.empty,
      `Newsletter ID` = getValue("concatId"),
      `Newsletter Name` = Option.empty,
      Owner = Option.empty,
      Quantity = Option.empty,
      `Record Type` = Option.empty,
      Status = Option.empty,
      `Status Opt Out` = Option.empty,
      Subscribed = getValue("hasSubscription", BooleanToYNConverter),
      `Subscription Confirmed` = getValue("hasConfirmedSubscription", BooleanToYNConverter),
      `Subscription Confirmed Date` = getValue("confirmedSubscriptionDate"),
      `Subscription Date` = getValue("subscriptionDate"),
      `Subscription Type` = getValue("subscriptionType"),
      `Unsubscription Confirmed Date` = getValue("confirmedSubscriptionDate"),
      `Unsubscription Date` = getValue("subscriptionDate"),
      Comment = Option.empty
    )
  }
}
