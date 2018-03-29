package com.unilever.ohub.spark.tsv2parquet

import com.unilever.ohub.spark.domain.DomainEntity.IngestionError
import com.unilever.ohub.spark.domain.MandatoryFieldException
import org.apache.spark.sql.Row

object DomainTransformer {
  def apply(): DomainTransformer = new DomainTransformer()
}

class DomainTransformer extends Serializable {
  var errors: Map[String, IngestionError] = Map()

  def mandatory(originalColumnName: String, domainFieldName: String)(implicit row: Row): String =
    mandatory(originalColumnName, domainFieldName, identity)(row)

  def mandatory[T](originalColumnName: String, domainFieldName: String, transformFn: String => T)(implicit row: Row): T = {
    val originalValueFn: Row => Option[String] = originalValue(originalColumnName)

    readAndTransform[T](originalColumnName, domainFieldName, mandatory = true, originalValueFn, transformFn)(row).get
  }

  def optional(originalColumnName: String, domainFieldName: String)(implicit row: Row): Option[String] = {
    val originalValueFn: Row => Option[String] = originalValue(originalColumnName)

    readAndTransform(originalColumnName, domainFieldName, mandatory = false, originalValueFn, identity)(row)
  }

  def optional[T](originalColumnName: String, domainFieldName: String, transformFn: String => T)(implicit row: Row): Option[T] = {
    val originalValueFn: Row => Option[String] = originalValue(originalColumnName)

    readAndTransform(originalColumnName, domainFieldName, mandatory = false, originalValueFn, transformFn)(row)
  }

  private def readAndTransform[T](originalColumnName: String, domainFieldName: String, mandatory: Boolean, originalValueFn: Row => Option[String], transformFn: String => T)(implicit row: Row): Option[T] = {
    val valueOpt: Option[String] = originalValueFn(row)

    if (mandatory && valueOpt.isEmpty) {
      throw MandatoryFieldException(domainFieldName, s"No value found for '$originalColumnName'")
    }

    try {
      valueOpt.map(transformFn)
    } catch {
      case e: Exception =>
        if (mandatory) {
          throw MandatoryFieldException(domainFieldName, s"Couldn't apply transformation function on value '$valueOpt'")
        } else {
          val ingestionError = IngestionError(
            originalColumnName = originalColumnName,
            inputValue = valueOpt,
            exceptionMessage = s"${e.getClass.getName}:${e.getMessage}"
          )
          errors = errors.updated(domainFieldName, ingestionError)
        }
        None
    }
  }

  def originalValue(columnName: String)(row: Row): Option[String] = {
    val fieldIndex = row.fieldIndex(columnName)
    Option(row.getString(fieldIndex)).filterNot(_.trim.isEmpty) // treat empty strings as None
  }
}
