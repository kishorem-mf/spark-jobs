package com.unilever.ohub.spark.tsv2parquet

import com.unilever.ohub.spark.domain.DomainEntity.IngestionError
import com.unilever.ohub.spark.domain.MandatoryFieldException
import org.apache.spark.sql.Row

object DomainTransformer {
  def apply(): DomainTransformer = new DomainTransformer()
}

class DomainTransformer extends Serializable {
  var errors: Map[String, IngestionError] = Map()

  def mandatory[T](originalColumnName: String, domainFieldName: String)(implicit row: Row): T =
    try {
      row.getAs[T](originalColumnName)
    } catch {
      case e: Exception => throw new MandatoryFieldException(s"Mandatory field constraint for '$domainFieldName' not met", e)
    }

  def optional(originalColumnName: String, domainFieldName: String)(row: Row): Option[String] = {
    val originalValueFn: Row => Option[String] = originalValue(originalColumnName)
    val transformFn: String => String = result => result

    optional(originalColumnName, domainFieldName, originalValueFn, transformFn)(row)
  }

  def optional[T](originalColumnName: String, domainFieldName: String, transformFn: String => T)(row: Row): Option[T] = {
    val originalValueFn: Row => Option[String] = originalValue(originalColumnName)
    optional(originalColumnName, domainFieldName, originalValueFn, transformFn)(row)
  }

  def optional[T](originalColumnName: String, domainFieldName: String, originalValueFn: Row => Option[String], transformFn: String => T)(row: Row): Option[T] = {
    // needs to be outside try, will bubble up in errors when original value function has programming errors.
    val valueOpt: Option[String] = originalValueFn(row)

    try {
      valueOpt.map(transformFn) // invoking actual transform function is inside try (since when the value can't be transformed it's an error result)
    } catch {
      case e: Exception => // can only reach catch block when transformation function throws exception
        val ingestionError = IngestionError(
          originalColumnName = originalColumnName,
          inputValue = valueOpt.get,
          exceptionMessage = s"${e.getClass.getName}:${e.getMessage}"
        )
        errors = errors.updated(domainFieldName, ingestionError)
        None
    }
  }

  private def originalValue(columnName: String)(row: Row): Option[String] =
      Option(row.getAs[String](columnName)).filterNot(_.trim.isEmpty) // treat empty strings as None

  // we could treat additional fields differently by creating an 'additional' method here
}
