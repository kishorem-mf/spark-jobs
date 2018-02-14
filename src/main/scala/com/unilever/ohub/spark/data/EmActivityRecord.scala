package com.unilever.ohub.spark.data

  case class EmActivityRecord(
    activityId: Option[Long],
    countryCode: Option[String],
    webServiceRequestId: Option[String],
    actionType: Option[Long],
    activityName: Option[String],
    activityDetails: Option[String])
