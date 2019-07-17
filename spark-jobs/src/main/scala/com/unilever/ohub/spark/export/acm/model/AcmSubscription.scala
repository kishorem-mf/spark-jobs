package com.unilever.ohub.spark.export.acm.model

import com.unilever.ohub.spark.export.OutboundEntity

case class AcmSubscription(
                            SUBSCRIPTION_ID: String,
                            CP_LNKD_INTEGRATION_ID: String,
                            COUNTRY_CODE: String,
                            SUBSCRIBE_FLAG: String,
                            SERVICE_NAME: String,
                            DATE_CREATED: String,
                            DATE_UPDATED: String,
                            SUBSCRIPTION_EMAIL_ADDRESS: String = "",
                            SUBSCRIPTION_DATE: String,
                            SUBSCRIPTION_CONFIRMED: String,
                            SUBSCRIPTION_CONFIRMED_DATE: String,
                            FAIR_KITCHENS_SIGN_UP_TYPE: String,
                            COMMUNICATION_CHANNEL: String,
                            SUBSCRIPTION_TYPE: String,
                            DELETED_FLAG: String,
                          ) extends OutboundEntity
