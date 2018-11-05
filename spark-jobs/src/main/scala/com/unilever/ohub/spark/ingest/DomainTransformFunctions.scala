package com.unilever.ohub.spark.ingest

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.DomainEntity
import org.apache.spark.sql.Row

trait DomainTransformFunctions { self: DomainTransformer ⇒

  def createConcatId(countryCodeColumn: String, sourceNameColumn: String, sourceEntityIdColumn: String)(implicit row: Row): String = {
    val countryCode: String = optionalValue(countryCodeColumn)(row).get
    val sourceName: String = optionalValue(sourceNameColumn)(row).get
    val sourceEntityId: String = optionalValue(sourceEntityIdColumn)(row).get

    DomainEntity.createConcatIdFromValues(countryCode, sourceName, sourceEntityId)
  }

  def currentTimestamp() = new Timestamp(System.currentTimeMillis())
}
