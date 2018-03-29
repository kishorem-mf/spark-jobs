package com.unilever.ohub.spark.domain

import com.unilever.ohub.spark.domain.DomainEntity.IngestionError

object DomainEntity {
  case class IngestionError(originalColumnName: String, inputValue: Option[String], exceptionMessage: String)
}

// marker trait for all domain entities (press ctrl + h in IntelliJ to see all)
trait DomainEntity extends Product {
  val sourceEntityId: String
  val sourceName: String       // UFT-8 characters: existing OHUB source name
  val countryCode: String      // Existing country code in OHUB using: Iso 3166-1 alpha 2
  val isActive: Boolean        // A | D
  val name: String

  val concatId: String = s"$countryCode~$sourceName~$sourceEntityId" // derived from mandatory fields

  val ingestionErrors: Map[String, IngestionError]

  assert(ingestionErrors.isEmpty, s"can't create domain entity due to '${ingestionErrors.size}' ingestion error(s): '$ingestionErrors'")
}
