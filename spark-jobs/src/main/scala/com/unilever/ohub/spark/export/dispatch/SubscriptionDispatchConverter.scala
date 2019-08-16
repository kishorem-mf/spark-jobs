package com.unilever.ohub.spark.export.dispatch

import com.unilever.ohub.spark.domain.entity.Subscription
import com.unilever.ohub.spark.export.dispatch.model.DispatchSubscription
import com.unilever.ohub.spark.export.{BooleanTo10Converter, Converter, InvertedBooleanToYNConverter, TypeConversionFunctions}

object SubscriptionDispatchConverter extends Converter[Subscription, DispatchSubscription] with TypeConversionFunctions with DispatchTransformationFunctions {

  override def convert(implicit subscription: Subscription, explain: Boolean = false): DispatchSubscription = {
    DispatchSubscription(
      CP_ORIG_INTEGRATION_ID = getValue("contactPersonConcatId"),
      SUBSCR_INTEGRATION_ID = getValue("concatId"),
      COUNTRY_CODE = getValue("countryCode"),
      CREATED_AT = getValue("ohubCreated"),
      UPDATED_AT = getValue("ohubUpdated"),
      DELETE_FLAG = getValue("isActive", InvertedBooleanToYNConverter),
      NL_NAME = getValue("subscriptionType"),
      REGION = getValue("countryCode"),
      SUBSCRIBED = getValue("hasSubscription", BooleanTo10Converter),
      SUBSCRIPTION_DATE = getValue("subscriptionDate"),
      SUBSCRIPTION_CONFIRMED = getValue("hasConfirmedSubscription", BooleanTo10Converter),
      SUBSCRIPTION_CONFIRMED_DATE = getValue("confirmedSubscriptionDate")
    )
  }
}
