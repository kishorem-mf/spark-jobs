package com.unilever.ohub.spark.ingest

import java.sql.{Date, Timestamp}
import java.time._

import org.apache.log4j.{LogManager, Logger}
import java.time.format.{DateTimeFormatter, DateTimeParseException}


object CustomParsers {
  implicit protected val log: Logger = LogManager.getLogger(CustomParsers.getClass)

  def parseDateTimeUnsafe(dateTimePattern: String = "yyyyMMdd HH:mm:ss")(input: String): Timestamp = {
    try {
      val formatter = DateTimeFormatter.ofPattern("[yyyy-MM-dd'T'HH:mm:ss.SSSX]"
        + "[yyyy-MM-dd'T'HH:mm:ss.SSS]"
        + "[yyyyMMdd HH:mm:ss]"
        + "[yyyy-MM-dd'T'HH:mm:ss]"
        + "[yyyy-MM-dd'T'HH:mm]"
        + "[yyyy-MM-dd HH:mm:ss.SS]"
        + "[yyyyMMddHHmmss]"
        + "[dd/MM/yyyy HH:mm:ss]"
        + "[yyyy-MM-dd'T'HH:mm:ss.SSS'Z']"
        + "[yyyy-MM-dd'T'HH:mm:ss.SSSSSSS'Z']"
        + "[yyyyMMdd HH:mm:ss]"
      )
      val parsed = LocalDateTime.parse(input, formatter) // check whether it satisfies the supplied date time pattern (throws an exception if it doesn't)
      Timestamp.valueOf(parsed)
    } catch {
      case p: DateTimeParseException =>
        val format = DateTimeFormatter.ofPattern(dateTimePattern)
        val parsed = LocalDateTime.parse(input, format) // check whether it satisfies the supplied date time pattern (throws an exception if it doesn't)
        Timestamp.valueOf(parsed)
    }
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
