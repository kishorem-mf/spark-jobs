package com.unilever.ohub.spark.tsv2parquet

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.domain.DomainEntity.IngestionError
import org.apache.spark.sql.Row

trait DomainTransformFunctions { self: DomainTransformer ⇒

  def createConcatId(countryCodeColumn: String, sourceNameColumn: String, sourceEntityIdColumn: String)(implicit row: Row): String = {
    val countryCode: String = originalValue("COUNTRY_CODE")(row).get
    val sourceName: String = originalValue("SOURCE")(row).get
    val sourceEntityId: String = originalValue("REF_OPERATOR_ID")(row).get

    DomainEntity.createConcatIdFromValues(countryCode, sourceName, sourceEntityId)
  }

  def currentTimestamp() = new Timestamp(System.currentTimeMillis())

  def splitAddress(columnName: String, domainFieldName: String)(implicit row: Row): (Option[String], Option[String], Option[String]) = {
    val streetOpt = originalValue(columnName)(row)

    streetOpt.map { street ⇒
      val splittedStreet = street.split(" ")

      if (splittedStreet.size == 1) {
        (Some(splittedStreet(0)), None, None)
      } else if (splittedStreet.size == 2) {
        (Some(splittedStreet(0)), Some(splittedStreet(1)), None)
      } else if (splittedStreet.size == 3) {
        (Some(splittedStreet(0)), Some(splittedStreet(1)), Some(splittedStreet(2)))
      } else {
        val ingestionError = IngestionError(
          originalColumnName = columnName,
          inputValue = streetOpt,
          exceptionMessage = "Could not split address into street, houseNumber and extension"
        )
        errors = errors.updated(domainFieldName, ingestionError)
        (None, None, None)
      }
    }.getOrElse((None, None, None))
  }
}
