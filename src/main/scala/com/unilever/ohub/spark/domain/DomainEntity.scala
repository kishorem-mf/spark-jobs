package com.unilever.ohub.spark.domain

import com.unilever.ohub.spark.domain.DomainEntity.IngestionError

object DomainEntity {
  case class IngestionError(originalColumnName: String, inputValue: Option[String], exceptionMessage: String)
}

// marker trait for all domain entities (press ctrl + h in IntelliJ to see all)
trait DomainEntity extends Product {
  // mandatory fields
  val sourceEntityId: String
  val sourceName: String
  val countryCode: String
  val isActive: Boolean
  val name: String

  // used for grouping and marking the golden record within the group
  val groupId: Option[String]
  val isGoldenRecord: Boolean

  // derived fields
  val concatId: String = s"$countryCode~$sourceName~$sourceEntityId"

  // aggregated fields
  val ingestionErrors: Map[String, IngestionError]

  assert(ingestionErrors.isEmpty, s"can't create domain entity due to '${ingestionErrors.size}' ingestion error(s): '$ingestionErrors'")
}
