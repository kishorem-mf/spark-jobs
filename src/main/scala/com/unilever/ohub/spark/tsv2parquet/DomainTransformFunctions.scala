package com.unilever.ohub.spark.tsv2parquet

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.domain.DomainEntity.IngestionError
import org.apache.spark.sql.Row

trait DomainTransformFunctions { self: DomainTransformer ⇒

  def dataProvider: DomainDataProvider

  def createConcatId(countryCodeColumn: String, sourceNameColumn: String, sourceEntityIdColumn: String)(implicit row: Row): String = {
    val countryCode: String = originalValue(countryCodeColumn)(row).get
    val sourceName: String = originalValue(sourceNameColumn)(row).get
    val sourceEntityId: String = originalValue(sourceEntityIdColumn)(row).get

    DomainEntity.createConcatIdFromValues(countryCode, sourceName, sourceEntityId)
  }

  def currentTimestamp() = new Timestamp(System.currentTimeMillis())

  def countryName(countryCode: String): Option[String] = dataProvider.countries.get(countryCode).map(_.countryName)

  def countryCodeBySalesOrg(salesOrg: String): Option[String] = dataProvider.countrySalesOrg.get(salesOrg).map(_.countryCode)

  // TODO consider to use a lib for this
  def splitAddress(columnName: String, domainFieldName: String)(implicit row: Row): (Option[String], Option[String], Option[String]) = {
    val streetOpt = originalValue(columnName)(row)

    streetOpt.map { street ⇒
      val splitStreet = street.split(" ")

      if (splitStreet.size == 1) {
        (Some(splitStreet(0)), None, None)
      } else if (splitStreet.size == 2) {
        (Some(splitStreet(0)), Some(splitStreet(1)), None)
      } else if (splitStreet.size == 3) {
        (Some(splitStreet(0)), Some(splitStreet(1)), Some(splitStreet(2)))
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
