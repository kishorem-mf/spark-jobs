package com.unilever.ohub.spark.data

case class GoldenContactPersonRecord(
    ohubContactPersonId: String,
    contactPerson: ContactPersonRecord,
    refIds: Seq[String],
    countryCode: Option[String]
)
