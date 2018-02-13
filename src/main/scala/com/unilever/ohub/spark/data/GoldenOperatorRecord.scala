package com.unilever.ohub.spark.data

case class GoldenOperatorRecord(
  ohubOperatorId: String,
  operator: OperatorRecord,
  refIds: Seq[String],
  countryCode: String
)
