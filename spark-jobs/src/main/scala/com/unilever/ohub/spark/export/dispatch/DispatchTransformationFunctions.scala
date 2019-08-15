package com.unilever.ohub.spark.export.dispatch

import java.sql
import java.sql.Timestamp
import java.time.format.DateTimeFormatter

import com.unilever.ohub.spark.export.TypeConversionFunctions

trait DispatchTransformationFunctions extends TypeConversionFunctions {

  override protected[export] implicit def optionalTimestampToString(input: Option[Timestamp]): String = input.map(t ⇒ timestampToString(t)).getOrElse("")

  override protected[export] implicit def timestampToString(input: Timestamp) = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(input.toLocalDateTime)

  override protected[export] implicit def formatDateWithPattern(input: Option[sql.Date]): String = input.map(t ⇒ formatDateWithPattern(t)).getOrElse("")

  protected[export] implicit def formatDateWithPattern = (input: sql.Date) ⇒ DateTimeFormatter.ofPattern("yyyy-MM-dd").format(input.toLocalDate)
}
