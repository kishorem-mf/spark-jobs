package com.unilever.ohub.spark.export.acm

import com.unilever.ohub.spark.domain.entity.Subscription
import com.unilever.ohub.spark.export.acm.model.AcmSubscription
import com.unilever.ohub.spark.export.{Converter, TransformationFunctions}

object SubscriptionAcmConverter extends Converter[Subscription, AcmSubscription] with TransformationFunctions with AcmTransformationFunctions {

  override def convert(subscription: Subscription): AcmSubscription = {
    AcmSubscription(
      COUNTRY_CODE = subscription.countryCode,
      SUBSCRIBE_FLAG = booleanToYNConverter(subscription.hasSubscription),
      SERVICE_NAME = subscription.subscriptionType,
      DATE_CREATED = subscription.ohubCreated,
      DATE_UPDATED = subscription.ohubUpdated,
      SUBSCRIPTION_DATE = subscription.subscriptionDate,
      SUBSCRIPTION_CONFIRMED = subscription.hasConfirmedSubscription.booleanToYNU,
      SUBSCRIPTION_CONFIRMED_DATE = subscription.confirmedSubscriptionDate,
      FAIR_KITCHENS_SIGN_UP_TYPE = subscription.fairKitchensSignUpType,
      COMMUNICATION_CHANNEL = subscription.communicationChannel,
      SUBSCRIPTION_TYPE = subscription.subscriptionType,
      DELETED_FLAG = booleanToYNConverter(!subscription.isActive),
      CP_LNKD_INTEGRATION_ID = subscription.contactPersonOhubId,
      SUBSCRIPTION_ID = subscription.concatId
    )
  }
}
