package com.unilever.ohub.spark.domain

object DomainEntity {
  case class IngestionError(originalColumnName: String, inputValue: String, exceptionMessage: String)
}

// marker trait for all domain entities (press ctrl + h in IntelliJ to see all)
trait DomainEntity extends Product
