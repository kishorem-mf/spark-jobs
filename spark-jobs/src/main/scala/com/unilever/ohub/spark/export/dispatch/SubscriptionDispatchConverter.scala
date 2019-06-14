package com.unilever.ohub.spark.export.dispatch

import com.unilever.ohub.spark.domain.entity.Subscription
import com.unilever.ohub.spark.export.dispatch.model.DispatchSubscription
import com.unilever.ohub.spark.export.{Converter, TransformationFunctions}

object SubscriptionDispatchConverter extends Converter[Subscription, DispatchSubscription] with TransformationFunctions with DispatchTransformationFunctions {

  override def convert(subscription: Subscription): DispatchSubscription = {
    DispatchSubscription(
      CP_ORIG_INTEGRATION_ID = subscription.contactPersonConcatId,
      SUBSCR_INTEGRATION_ID = subscription.concatId,
      COUNTRY_CODE = subscription.countryCode,
      CREATED_AT = subscription.ohubCreated,
      UPDATED_AT = subscription.ohubUpdated,
      DELETE_FLAG = !subscription.isActive,
      NL_NAME = subscription.subscriptionType,
      REGION = subscription.countryCode,
      SUBSCRIBED = subscription.hasSubscription,
      SUBSCRIPTION_DATE = subscription.subscriptionDate,
      SUBSCRIPTION_CONFIRMED = subscription.hasConfirmedSubscription,
      SUBSCRIPTION_CONFIRMED_DATE = subscription.confirmedSubscriptionDate
    )
  }
}