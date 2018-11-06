package com.unilever.ohub.spark.domain.entity

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.domain.DomainEntity.IngestionError

case class Answer(
    // generic fields
    // mandatory fields
    concatId: String,
    countryCode: String,
    customerType: String,
    sourceEntityId: String,
    sourceName: String,
    isActive: Boolean,
    ohubCreated: Timestamp,
    ohubUpdated: Timestamp,
    // optional fields
    dateCreated: Option[Timestamp],
    dateUpdated: Option[Timestamp],
    // used for grouping and marking the golden record within the group
    ohubId: Option[String],
    isGoldenRecord: Boolean,

    // specific fields
    answer: Option[String],
    questionConcatId: String,

    // other fields
    additionalFields: Map[String, String],
    ingestionErrors: Map[String, IngestionError]
) extends DomainEntity {}
