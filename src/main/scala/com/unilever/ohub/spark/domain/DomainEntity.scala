package com.unilever.ohub.spark.domain

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.DomainEntity.IngestionError

object DomainEntity {
  case class IngestionError(originalColumnName: String, inputValue: Option[String], exceptionMessage: String)
}

// marker trait for all domain entities (press ctrl + h in IntelliJ to see all)
trait DomainEntity extends Product {
  // mandatory fields
  val concatId: String
  val sourceEntityId: String
  val sourceName: String
  val countryCode: String
  val isActive: Boolean
  val name: String
  val ohubCreated: Timestamp
  val ohubUpdated: Timestamp

  // used for grouping and marking the golden record within the group
  val ohubId: Option[String]
  val isGoldenRecord: Boolean


  // aggregated fields
  val ingestionErrors: Map[String, IngestionError]

  assert(ingestionErrors.isEmpty, s"can't create domain entity due to '${ingestionErrors.size}' ingestion error(s): '$ingestionErrors'")
}
