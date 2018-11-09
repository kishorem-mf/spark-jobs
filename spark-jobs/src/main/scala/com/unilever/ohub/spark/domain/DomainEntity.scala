package com.unilever.ohub.spark.domain

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.DomainEntity.IngestionError

object DomainEntity {
  case class IngestionError(originalColumnName: String, inputValue: Option[String], exceptionMessage: String)

  def createConcatIdFromValues(countryCode: String, sourceName: String, sourceEntityId: String): String =
    s"$countryCode~$sourceName~$sourceEntityId"
}

// marker trait for all domain entities (press ctrl + h in IntelliJ to see all)
trait DomainEntity extends Product {
  // mandatory fields
  val concatId: String
  val countryCode: String
  val customerType: String
  val sourceEntityId: String
  val sourceName: String
  val isActive: Boolean
  val ohubCreated: Timestamp
  val ohubUpdated: Timestamp

  // optional fields
  val dateCreated: Option[Timestamp]
  val dateUpdated: Option[Timestamp]

  // used for grouping and marking the golden record within the group
  val ohubId: Option[String]
  val isGoldenRecord: Boolean

  // aggregated fields
  val additionalFields: Map[String, String]
  val ingestionErrors: Map[String, IngestionError]

  // ENABLE IF THE ENTITY SHOULDN'T BE CREATED WHEN INGESTION ERRORS ARE PRESENT
  // assert(ingestionErrors.isEmpty, s"can't create domain entity due to '${ingestionErrors.size}' ingestion error(s): '${ingestionErrors.keySet.toSeq.sorted.mkString(",")}'")
}