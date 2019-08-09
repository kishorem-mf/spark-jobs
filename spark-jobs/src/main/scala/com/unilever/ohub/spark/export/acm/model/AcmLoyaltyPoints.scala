package com.unilever.ohub.spark.export.acm.model

import com.unilever.ohub.spark.export.OutboundEntity

case class AcmLoyaltyPoints(
                             CP_ORIG_INTEGRATION_ID: String,
                             COUNTRY_CODE: String,
                             CP_LNKD_INTEGRATION_ID: String,
                             EMAIL_ADDRESS: String = "",
                             EARNED: String,
                             SPENT: String,
                             ACTUAL: String,
                             GOAL: String,
                             UPDATED_AT: String,
                             REWARD_NAME: String,
                             REWARD_IMAGE_URL: String,
                             REWARD_LDP_URL: String,
                             REWARD_EANCODE: String) extends OutboundEntity
                             //LOYALTY_POINT_ID: String)
