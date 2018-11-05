package com.unilever.ohub.spark.domain.entity

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.domain.DomainEntity.IngestionError

case class Activity(
    // generic fields
    // mandatory fields
    val concatId: String,
    val countryCode: String,
    val customerType: String,
    val sourceEntityId: String,
    val sourceName: String,
    val isActive: Boolean,
    val ohubCreated: Timestamp,
    val ohubUpdated: Timestamp,
    // optional fields
    val dateCreated: Option[Timestamp],
    val dateUpdated: Option[Timestamp],
    // used for grouping and marking the golden record within the group
    val ohubId: Option[String],
    val isGoldenRecord: Boolean,

    // specific fields
    val activityDate: Option[Timestamp],
    val name: Option[String],
    val details: Option[String],
    val actionType: Option[String],
    val contactPersonConcatId: Option[String],
    val contactPersonOhubId: Option[String],

    // other fields
    val additionalFields: Map[String, String],
    val ingestionErrors: Map[String, IngestionError]
) extends DomainEntity {}
