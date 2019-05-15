package com.unilever.ohub.spark.export.acm

import java.sql
import java.sql.Timestamp
import java.time.format.DateTimeFormatter

import com.unilever.ohub.spark.export.TransformationFunctions

trait AcmTransformationFunctions extends TransformationFunctions {

  protected[acm] implicit def optionalTimestampToString(input: Option[Timestamp]): String = input.map(t ⇒ formatWithPattern(t)).getOrElse("")

  protected[acm] implicit def formatDateWithPattern(input: Option[sql.Date]): String = input.map(t ⇒ DateTimeFormatter.ofPattern("yyyy/MM/dd").format(t.toLocalDate)).getOrElse("")

  protected[acm] implicit def formatWithPattern(input: Timestamp): String = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss").format(input.toLocalDateTime)

}
