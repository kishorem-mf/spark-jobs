package com.unilever.ohub.spark.data

  case class EmContactRequestRecord(
    contactRequestId: Option[Long],
    countryCode: Option[String],
    webServiceRequestId: Option[Long],
    contactMessage: Option[String],
    happy: Option[String],
    requestType: Option[Long]) // enum
