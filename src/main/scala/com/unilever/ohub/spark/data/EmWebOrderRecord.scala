package com.unilever.ohub.spark.data

import java.sql.Timestamp

  case class EmWebOrderRecord(
    countryCode: Option[String],
    transactionDate: Option[Timestamp],
    orderAmount: Option[Long],
  )
