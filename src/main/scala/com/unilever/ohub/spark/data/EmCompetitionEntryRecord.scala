package com.unilever.ohub.spark.data

  case class EmCompetitionEntryRecord(
    competitionEntryId: Option[Long],
    countryCode: Option[String],
    webServiceRequestId: Option[Long],
    competitionName: Option[String],
    mediaName: Option[String])
