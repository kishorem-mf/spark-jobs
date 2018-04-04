package com.unilever.ohub.spark.tsv2parquet

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.DomainEntity.IngestionError
import org.apache.spark.sql.Row

object DomainTransformer {
  def apply(): DomainTransformer = new DomainTransformer()

  val ZERO_WIDTH_NO_BREAK_SPACE = "\uFEFF" // see also: http://www.fileformat.info/info/unicode/char/FEFF/index.htm
}

class DomainTransformer extends Serializable {
  import DomainTransformer._

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
    val fieldIndex = getFieldIndex(columnName)(row)
    Option(row.getString(fieldIndex)).filterNot(_.trim.isEmpty) // treat empty strings as None
  }

  private def getFieldIndex(columnName: String)(row: Row): Int =
    try {
      row.fieldIndex(columnName)
    } catch {
      // maybe there is a BOM char in front of the column name, otherwise let's fail.
      case _: Throwable => row.fieldIndex(s"$ZERO_WIDTH_NO_BREAK_SPACE$columnName")
    }

  // where to put the following functions

  def createConcatId(countryCodeColumn: String, sourceNameColumn: String, sourceEntityIdColumn: String)(implicit row: Row): String = {
    val countryCode: String = originalValue("COUNTRY_CODE")(row).get
    val sourceName: String = originalValue("SOURCE")(row).get
    val sourceEntityId: String = originalValue("REF_OPERATOR_ID")(row).get

    s"$countryCode~$sourceName~$sourceEntityId"
  }

  def currentTimestamp() = new Timestamp(System.currentTimeMillis())

}

object MandatoryFieldException {
  def apply(domainFieldName: String, errorMessage: String): MandatoryFieldException =
    new MandatoryFieldException(s"Mandatory field constraint for '$domainFieldName' not met: $errorMessage")
}

class MandatoryFieldException(message: String) extends IllegalArgumentException(message)
