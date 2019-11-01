package com.unilever.ohub.spark.export.dispatch.model

import com.unilever.ohub.spark.export.DispatcherOutboundEntity

case class DispatchSubscription(
    CP_ORIG_INTEGRATION_ID: String,
    SUBSCR_INTEGRATION_ID: String,
    COUNTRY_CODE: String,
    CREATED_AT: String,
    UPDATED_AT: String,
    DELETE_FLAG: String,
    NL_NAME: String,
    REGION: String,
    SUBSCRIBED: String,
    SUBSCRIPTION_DATE: String,
    SUBSCRIPTION_CONFIRMED: String,
    SUBSCRIPTION_CONFIRMED_DATE: String,
    SUBSCRIPTION_EMAIL_ADDRESS: String = "") extends DispatcherOutboundEntity
