package com.unilever.ohub.spark.ingest

import java.sql.{ Date, Timestamp }
import java.time._
import java.time.format.DateTimeFormatter

object CustomParsers {

  def parseDateTimeUnsafe(dateTimePattern: String = "yyyyMMdd HH:mm:ss")(input: String): Timestamp = {
    val pattern = DateTimeFormatter.ofPattern(dateTimePattern)
    val parsed = LocalDateTime.parse(input, pattern) // check whether it satisfies the supplied date time pattern (throws an exception if it doesn't)
    Timestamp.valueOf(parsed)
  }

  def parseDateUnsafe(datePattern: String = "yyyyMMdd")(input: String): Date = {
    val pattern = DateTimeFormatter.ofPattern(datePattern)
    val localDate = LocalDate.parse(input, pattern)
    val jdbcDatePattern = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val str = localDate.format(jdbcDatePattern)
    Date.valueOf(str)
  }

  def toTimestamp(input: String): Timestamp = new Timestamp(input.toLong)

  def toBoolean(input: String): Boolean = input.toBoolean

  def toInt: String ⇒ Int = input ⇒ input.toInt

  def toLong: String ⇒ Long = input ⇒ input.toLong

  def toBigDecimal: String ⇒ BigDecimal = input ⇒ BigDecimal(input)
}
