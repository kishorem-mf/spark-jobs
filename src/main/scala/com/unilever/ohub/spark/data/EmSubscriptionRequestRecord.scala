package com.unilever.ohub.spark.data

import java.sql.Timestamp

  case class EmSubscriptionRequestRecord(
    subscriptionRequestId: Option[Long],
    countryCode: Option[String], // enum
    webServiceRequestId: Option[Long],
    newsletter: Option[String], // enum: default_newsletter_opt_in
    subscribed: Option[Boolean], // 0/1
    subscriptionDate: Option[Timestamp],
    subscriptionConfirmed: Option[Boolean], // 0/1
    subscriptionConfirmedDate: Option[Timestamp])
