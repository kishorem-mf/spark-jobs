package com.unilever.ohub.spark.export.dispatch

import java.sql
import java.sql.Timestamp
import java.time.format.DateTimeFormatter

import com.unilever.ohub.spark.export.TransformationFunctions

trait DispatchTransformationFunctions extends TransformationFunctions {

  protected[dispatch] implicit def optionalTimestampToString(input: Option[Timestamp]): String = input.map(t ⇒ formatWithPattern(t)).getOrElse("")

  protected[dispatch] implicit def formatWithPattern(input: Timestamp) = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(input.toLocalDateTime)

  protected[dispatch] implicit def formatDateWithPattern(input: Option[sql.Date]): String = input.map(t ⇒ formatDateWithPattern(t)).getOrElse("")

  protected[dispatch] implicit def formatDateWithPattern = (input: sql.Date) ⇒ DateTimeFormatter.ofPattern("yyyy-MM-dd").format(input.toLocalDate)
}
