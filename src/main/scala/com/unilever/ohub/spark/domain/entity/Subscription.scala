package com.unilever.ohub.spark.domain.entity

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.domain.DomainEntity.IngestionError

object Subscription {
  val customerType = "SUBSCRIPTION"
}

case class Subscription(
    // generic fields
    concatId: String,
    countryCode: String,
    customerType: String,
    dateCreated: Option[Timestamp],
    dateUpdated: Option[Timestamp],
    isActive: Boolean,
    isGoldenRecord: Boolean,
    sourceEntityId: String,
    sourceName: String,
    ohubId: Option[String],
    ohubCreated: Timestamp,
    ohubUpdated: Timestamp,
    // specific fields
    //
    // other fields
    additionalFields: Map[String, String],
    ingestionErrors: Map[String, IngestionError]
) extends DomainEntity
