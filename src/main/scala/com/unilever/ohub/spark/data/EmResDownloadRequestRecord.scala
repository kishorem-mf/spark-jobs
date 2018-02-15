package com.unilever.ohub.spark.data

  case class EmResDownloadRequestRecord(
    resDownloadRequestId: Option[Long],
    countryCode: Option[String], // enum
    webServiceRequestId: Option[Long],
    downloadItem: Option[String])
