package com.unilever.ohub.spark.data

import java.sql.Timestamp

  case class EmWebserviceRequestRecord(
    webServiceRequestId: Option[Long],
    countryCode: Option[String], // enum
    username: Option[String], // enum: emakina
    requestIdentifier: Option[String],
    emSourceId: Option[String],
    dateReceived: Option[Timestamp],
    updateContext: Option[String], // enum: update_profile, register_profile
    operation: Option[String], // enum
    ipAddress: Option[String],
    processedFlag: Option[Boolean], // 0/1
    dateWodProcessed: Option[Timestamp],
    indSkipped: Option[Boolean], // Y/N
    endRequestDate: Option[Timestamp],
    compositeVersion: Option[String], // 1.2~1.4
    skipReason: Option[String], // enum: NOT_LATEST
    processId: Option[Long])
