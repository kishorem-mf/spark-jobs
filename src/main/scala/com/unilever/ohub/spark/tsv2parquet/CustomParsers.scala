package com.unilever.ohub.spark.tsv2parquet

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.log4j.Logger
import org.apache.spark.sql.Row

import scala.util.Try

object CustomParsers {
  object Implicits {
    implicit class RowOps(row: Row) {
      def parseStringOption(index: Int): Option[String] = {
        Try(row.getString(index))
          .toOption
          .flatMap(Option.apply) // turn null values into None
          .filter(_.nonEmpty) // turn empty strings into None
      }

      def parseDateTimeStampOption(index: Int)(implicit log: Logger): Option[Timestamp] = {
        parseStringOption(index).map(CustomParsers.parseDateTimeStampUnsafe)
      }

      def parseBigDecimalOption(index: Int): Option[BigDecimal] = {
        parseStringOption(index).map(CustomParsers.parseBigDecimalUnsafe)
      }

      def parseBigDecimalRangeOption(index: Int): Option[BigDecimal] = {
        parseStringOption(index).flatMap(CustomParsers.parseBigDecimalRangeOption)
      }

      def parseLongRangeOption(index: Int): Option[Long] = {
        parseStringOption(index).flatMap(CustomParsers.parseLongRangeOption)
      }

      def parseBooleanOption(index: Int): Option[Boolean] = {
        parseStringOption(index).map(CustomParsers.parseBoolUnsafe)
      }
    }
  }

  def parseDateTimeForPattern(dateTimePattern: String = "yyyy-MM-dd HH:mm:ss.SS")(input: String): Timestamp = {
    val pattern = DateTimeFormatter.ofPattern(dateTimePattern)
    LocalDateTime.parse(input, pattern) // check whether it satisfies the supplied date time pattern (throws an exception if it doesn't)
    Timestamp.valueOf(input)
  }

  private val timestampFormatter = new ThreadLocal[SimpleDateFormat]() {
    override protected def initialValue = new SimpleDateFormat("yyyyMMdd HH:mm:ss")
  }

  def parseDateTimeStampOption(input: String)(implicit log: Logger): Option[Timestamp] = {
    Try(parseDateTimeStampUnsafe(input)(log)).toOption
  }

  def parseDateTimeStampUnsafe(input: String)(implicit log: Logger): Timestamp = {
    val str = input match {
      case s: String if s.matches("[ /:\\d]+") && s.length == 19 ⇒
        s.replace("/", "")
      case s: String if s.matches("[ \\-:\\d]+") && s.length == 19 ⇒
        s.replace("-", "")
      case s: String if s.matches("[ \\.:\\d]+") && s.length == 19 ⇒
        s.replace(".", "")
      case s: String if s.matches("[ :\\d]+") && s.length == 17 ⇒
        s
      case s: String if s.matches("\\d{8}") ⇒ // yyyyMMdd
        s.concat(" 00:00:00")
      case s: String if s.matches("\\d{2}-\\d{2}-\\d{2}") ⇒ // dd-MM-yy
        ("20" + s.takeRight(2) + s.slice(3, 5) + s.take(2)).concat(" 00:00:00")
      case s: String ⇒
        throw new IllegalArgumentException(s"Could not parse [$s] as DateTimeStampOption")
        s
    }
    new Timestamp(timestampFormatter.get.parse(str).getTime)
  }

  def parseDateUnsafe(formatStr: String) = {
    val format = new SimpleDateFormat(formatStr)
    (s: String) ⇒ new Timestamp(format.parse(s).getTime)
  }

  // formats for dates not in standard format yyyy-MM-dd HH:mm:ss
  val fuzzitFileNameDate = "yyyyMMddHHmmss"
  val fileShortDate = "yyyyMMdd" // can also use below format including time
  val ufsDate = "yyyyMMdd HH:mm:ss" // sifu api, file interface
  val sifuDate = "dd-MM-yy" // sifu?
  val acmDate = "dd/MM/yyyy HH:mm:ss"

  // parsers for dates not in standard format yyyy-MM-dd HH:mm:ss
  val parseFuzzitFileNameDateUnsafe = parseDateUnsafe(fuzzitFileNameDate)
  val parseFileShortDateUnsafe = parseDateUnsafe(fileShortDate)
  val parseUfsDateUnsafe = parseDateUnsafe(ufsDate) // sifu, file interface
  val parseSifuDateUnsafe = parseDateUnsafe(sifuDate)
  val parseAcmDateUnsafe = parseDateUnsafe(acmDate)

  def parseBigDecimalUnsafe(input: String): BigDecimal = input match {
    case inputString: String if inputString.matches("[-,0-9]+") ⇒ BigDecimal(inputString.replace(",", "."))
    case inputString: String if inputString.matches("[-.0-9]+") ⇒ BigDecimal(input)
  }

  private val numberRegex = "(-?\\d+)[\\.,]?\\d*".r
  private val numberRangeRegex = "(\\d+)-(\\d+)".r

  def parseLongRangeOption(input: String): Option[Long] = {
    input match {
      case ""                                         ⇒ None
      case numberRegex(longString)                    ⇒ Some(longString.toLong)
      case numberRangeRegex(longString1, longString2) ⇒ Some((longString1.toLong + longString2.toLong) / 2)
      case _                                          ⇒ None
    }
  }

  private val currencies = "\u0024\u00A2\u00A3\u00A4\u00A5\u058F\u060B\u09F2\u09F3\u09FB\u0AF1\u0BF9\u0E3F\u17DB\u20A0\u20A1\u20A2\u20A3\u20A4\u20A5\u20A6\u20A7\u20A8\u20A9\u20AA\u20AB\u20AC\u20AD\u20AE\u20AF\u20B0\u20B1\u20B2\u20B3\u20B4\u20B5\u20B6\u20B7\u20B8\u20B9\u20BA\u20BB\u20BC\u20BD\u20BE\uA838\uFDFC\uFE69\uFF04\uFFE0\uFFE1\uFFE5\uFFE6\u0081"
  private val doubleRegex = s"[$currencies]?([\\d.]+)".r
  private val doubleRangeRegex = s"[$currencies]?([\\d.]+)-[$currencies]?([\\d.]+)".r

  def parseBigDecimalRangeOption(input: String): Option[BigDecimal] = {
    input match {
      case doubleRegex(bigDecimalString) ⇒
        Some(BigDecimal(bigDecimalString))
      case doubleRangeRegex(bigDecimalString1, bigDecimalString2) ⇒
        Some((BigDecimal(bigDecimalString1) + BigDecimal(bigDecimalString2)) / 2)
      case _ ⇒
        None
    }
  }

  def parseBoolUnsafe(input: String): Boolean = {
    input.toUpperCase match {
      case "Y"                   ⇒ true
      case "N"                   ⇒ false
      case "A"                   ⇒ true
      case "D"                   ⇒ false
      case "X"                   ⇒ true
      case "1"                   ⇒ true
      case "0"                   ⇒ false
      case "TRUE"                ⇒ true
      case "FALSE"               ⇒ false
      case "YES"                 ⇒ true
      case "NO"                  ⇒ false
      /* Capturing strange cases from data source DEX begin*/
      case "DIRECTOR COMPRAS"    ⇒ true
      case "RESPONSIBLE FOOD"    ⇒ true
      case "RESPONSIBLE TEA"     ⇒ true
      case "RESPONSIBLE GENERAL" ⇒ true
      case "OTHER"               ⇒ true
      /* Capturing strange cases from data source DEX end*/
      case s: String ⇒
        throw new Exception(s"Could not parse [$s] as Boolean")
        false
    }
  }

  def longToBool(i: Long): Boolean = i > 0

  def intToBool(i: Int): Boolean = i > 0

  def floatToDate(f: Float): Timestamp = new Timestamp((1000 * f).toLong)

  def dateToString(t: Timestamp): String = t.formatted("yyyy-MM-dd HH:mm:ss")

  def boolAsString(bool: Boolean): String = if (bool) "Y" else "N"

  def withinRange(range: Range)(input: String): Int = {
    val value = input.toInt

    if (range.contains(value)) {
      value
    } else {
      throw new IllegalArgumentException(s"Input value '$input' not within provided range '$range'")
    }
  }

  def toInt(input: String): Int = input.toInt

  def parseNumberOrAverageFromRange(input: String): Int =
    input match {
      case numberRegex(number)          ⇒ number.toInt
      case numberRangeRegex(start, end) ⇒ Math.round(start.toInt + end.toInt / 2)
    }

  def parseBigDecimalOrAverageFromRange(input: String): BigDecimal =
    input match {
      case doubleRegex(bigDecimalString)                          ⇒ BigDecimal(bigDecimalString)
      case doubleRangeRegex(bigDecimalString1, bigDecimalString2) ⇒ (BigDecimal(bigDecimalString1) + BigDecimal(bigDecimalString2)) / 2
    }
}
