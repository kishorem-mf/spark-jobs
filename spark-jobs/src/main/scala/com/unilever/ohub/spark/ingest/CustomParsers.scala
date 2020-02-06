package com.unilever.ohub.spark.ingest

import java.sql.{ Date, Timestamp }
import java.time._
import java.time.format.DateTimeFormatter
import org.apache.log4j.{ LogManager, Logger }

object CustomParsers {
  implicit protected val log: Logger = LogManager.getLogger(CustomParsers.getClass)

  def parseDateTimeUnsafe(dateTimePattern: String = "yyyyMMdd HH:mm:ss")(input: String): Timestamp = {
    var pattern = DateTimeFormatter.ofPattern(dateTimePattern)
    var newInput = ""
    if (input.contains("T")) {
      newInput = input.replace("T", " ").replace("-", "")
      val last = newInput.split("\\s").last
      if (last.length == 5) {
        pattern = DateTimeFormatter.ofPattern("yyyyMMdd HH:mm")
      }
      val parsed = LocalDateTime.parse(newInput, pattern) // check whether it satisfies the supplied date time pattern (throws an exception if it doesn't)
      log.info(s"pattern [$pattern] newInput [$newInput]")
      Timestamp.valueOf(parsed)
    } else {
      val parsed = LocalDateTime.parse(input, pattern) // check whether it satisfies the supplied date time pattern (throws an exception if it doesn't)
      log.info(s"Again pattern [$pattern] newInput [$input]")
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
