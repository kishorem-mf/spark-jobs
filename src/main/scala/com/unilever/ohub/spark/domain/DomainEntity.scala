package com.unilever.ohub.spark.domain

import com.unilever.ohub.spark.domain.DomainEntity.IngestionError

object DomainEntity {
  case class IngestionError(originalColumnName: String, inputValue: Option[String], exceptionMessage: String)
}

// marker trait for all domain entities (press ctrl + h in IntelliJ to see all)
trait DomainEntity extends Product {
  val ingestionErrors: Map[String, IngestionError]

  assert(ingestionErrors.isEmpty, s"can't create domain entity due to '${ingestionErrors.size}' ingestion errors: '$ingestionErrors'")
}
