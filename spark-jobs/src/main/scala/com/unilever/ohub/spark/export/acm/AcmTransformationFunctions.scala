package com.unilever.ohub.spark.export.acm

import java.sql
import java.sql.Timestamp
import java.time.format.DateTimeFormatter

import com.unilever.ohub.spark.export.TypeConversionFunctions

trait AcmTransformationFunctions extends TypeConversionFunctions {

//  override protected[export] implicit def optionalTimestampToString(input: Option[Timestamp]): String = input.map(t ⇒ timestampToString(t)).getOrElse("")

  override protected[export] implicit def formatDateWithPattern(input: sql.Date): String = DateTimeFormatter.ofPattern("yyyy/MM/dd").format(input.toLocalDate)

  override protected[export] implicit def timestampToString(input: Timestamp): String = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss").format(input.toLocalDateTime)

}
