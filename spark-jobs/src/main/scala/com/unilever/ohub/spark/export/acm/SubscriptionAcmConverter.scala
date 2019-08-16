package com.unilever.ohub.spark.export.acm

import com.unilever.ohub.spark.domain.entity.Subscription
import com.unilever.ohub.spark.export.acm.model.AcmSubscription
import com.unilever.ohub.spark.export.{BooleanToYNConverter, BooleanToYNUCoverter, Converter, InvertedBooleanToYNConverter, TypeConversionFunctions}

object SubscriptionAcmConverter extends Converter[Subscription, AcmSubscription] with TypeConversionFunctions with AcmTransformationFunctions {

  override def convert(subscription: Subscription): AcmSubscription = {
    implicit val subsc = subscription

    AcmSubscription(
      COUNTRY_CODE = getValue("countryCode"),
      SUBSCRIBE_FLAG = getValue("hasSubscription", Some(BooleanToYNConverter)),
      DATE_CREATED = getValue("ohubCreated"),
      DATE_UPDATED = getValue("ohubUpdated"),
      SUBSCRIPTION_DATE = getValue("subscriptionDate"),
      SUBSCRIPTION_CONFIRMED = getValue("hasConfirmedSubscription", Some(BooleanToYNUCoverter)),
      SUBSCRIPTION_CONFIRMED_DATE = getValue("confirmedSubscriptionDate"),
      FAIR_KITCHENS_SIGN_UP_TYPE = getValue("fairKitchensSignUpType"),
      COMMUNICATION_CHANNEL = getValue("communicationChannel"),
      SUBSCRIPTION_TYPE = getValue("subscriptionType"),
      DELETED_FLAG = getValue("isActive", Some(InvertedBooleanToYNConverter)),
      CP_LNKD_INTEGRATION_ID = getValue("contactPersonOhubId"),
      SUBSCRIPTION_ID = getValue("concatId")
    )
  }
}
