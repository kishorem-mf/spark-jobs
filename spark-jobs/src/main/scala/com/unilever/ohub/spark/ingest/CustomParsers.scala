package com.unilever.ohub.spark.ingest

import java.sql.Timestamp
import java.time._
import java.time.format.DateTimeFormatter

object CustomParsers {

  def parseDateTimeUnsafe(dateTimePattern: String = "yyyyMMdd HH:mm:ss")(input: String): Timestamp = {
    val pattern = DateTimeFormatter.ofPattern(dateTimePattern)
    val parsed = LocalDateTime.parse(input, pattern) // check whether it satisfies the supplied date time pattern (throws an exception if it doesn't)
    Timestamp.valueOf(parsed)
  }

  def parseDateUnsafe(datePattern: String = "yyyyMMdd")(input: String): Timestamp = {
    val pattern = DateTimeFormatter.ofPattern(datePattern)
    val parsed = LocalDate.parse(input, pattern).atStartOfDay() // check whether it satisfies the supplied date pattern (throws an exception if it doesn't)
    Timestamp.valueOf(parsed)
  }

  def toTimestamp(input: String): Timestamp = new Timestamp(input.toLong)

  def toBoolean(input: String): Boolean = input.toBoolean

  def toInt: String ⇒ Int = input ⇒ input.toInt

  def toLong: String ⇒ Long = input ⇒ input.toLong

  def toBigDecimal: String ⇒ BigDecimal = input ⇒ BigDecimal(input)
}
