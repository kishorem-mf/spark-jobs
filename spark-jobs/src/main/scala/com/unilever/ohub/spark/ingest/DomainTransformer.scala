package com.unilever.ohub.spark.ingest

import com.unilever.ohub.spark.domain.DomainEntity.IngestionError
import org.apache.spark.sql.Row

object DomainTransformer {
  def apply(): DomainTransformer = new DomainTransformer()

  final val ZERO_WIDTH_NO_BREAK_SPACE = "\uFEFF" // see also: http://www.fileformat.info/info/unicode/char/FEFF/index.htm
}

class DomainTransformer() extends Serializable {

  import DomainTransformer._

  var headers: Map[String, Int] = Map()
  var additionalFields: Map[String, String] = Map()
  var errors: Map[String, IngestionError] = Map()

  def useHeaders(headers: Map[String, Int]): Unit = {
    this.headers = headers
  }

  def mandatory(originalColumnName: String)(implicit row: Row): String = {
    mandatory(originalColumnName, (x: String) => x)(row)
  }

  def mandatory[T](originalColumnName: String, transformFn: String ⇒ T)(implicit row: Row): T = {
    val valueOpt: Option[String] = optionalValue(originalColumnName)(row)

    transformOrError[T](originalColumnName, mandatory = true, valueOpt, transformFn).get
  }

  def optional(originalColumnName: String)(implicit row: Row): Option[String] = {
    val valueOpt: Option[String] = optionalValue(originalColumnName)(row)

    transformOrError(originalColumnName, mandatory = false, valueOpt, identity)
  }

  def optional[T](originalColumnName: String, transformFn: String ⇒ T)(implicit row: Row): Option[T] = {
    val valueOpt: Option[String] = optionalValue(originalColumnName)(row)

    transformOrError(originalColumnName, mandatory = false, valueOpt, transformFn)
  }

  private def transformOrError[T](originalColumnName: String, mandatory: Boolean,
                                  valueOpt: Option[String], transformFn: String ⇒ T): Option[T] = {
    if (mandatory && valueOpt.isEmpty) {
      throw MandatoryFieldException(originalColumnName, s"No value found for '$originalColumnName'")
    }

    try {
      valueOpt.map(transformFn)
    } catch {
      case e: Exception ⇒
        if (mandatory) {
          throw MandatoryFieldException(originalColumnName, s"Couldn't apply transformation function on value '$valueOpt'")
        } else {
          val ingestionError = IngestionError(
            originalColumnName = originalColumnName,
            inputValue = valueOpt,
            exceptionMessage = s"${e.getClass.getName}:${e.getMessage}"
          )
          errors = errors.updated(originalColumnName, ingestionError)
        }
        None
    }
  }

  def optionalValue(columnName: String)(row: Row): Option[String] = {
    try {
      val fieldIndex = getFieldIndex(columnName)(row)
      Option(row.getString(fieldIndex)).filterNot(_.trim.isEmpty) // treat empty strings as None
    } catch {
      case e: IllegalArgumentException ⇒
        if (e.getMessage contains "does not exist.") None else throw e
    }
  }

  private def getFieldIndex(columnName: String, numberOfBomCharsToPrepend: Int = 0)(implicit row: Row): Int =
    if (headers.isEmpty) {
      try {
        val bomChars = (0 until numberOfBomCharsToPrepend).map(_ ⇒ ZERO_WIDTH_NO_BREAK_SPACE).mkString("")
        val newColumnName = if (numberOfBomCharsToPrepend == 0) columnName else bomChars + "\"" + columnName + "\""

        row.fieldIndex(newColumnName)
      } catch {
        case e: Throwable ⇒
          // maybe there are n BOM chars in front of the column name (with n < 100), otherwise let's fail.
          if (numberOfBomCharsToPrepend < 100) {
            getFieldIndex(columnName, numberOfBomCharsToPrepend + 1)(row)
          } else {
            throw e
          }
      }
    } else {
      headers(columnName)
    }
}

object MandatoryFieldException {
  def apply(domainFieldName: String, errorMessage: String): MandatoryFieldException =
    new MandatoryFieldException(s"Mandatory field constraint for '$domainFieldName' not met: $errorMessage")
}

class MandatoryFieldException(message: String) extends IllegalArgumentException(message)
