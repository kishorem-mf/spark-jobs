package com.unilever.ohub.spark.tsv2parquet

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.log4j.Logger

object CustomParsers {

//  TODO Complete tests in CustomParsersSpec for new parsers

  private val timestampFormatter = new ThreadLocal[SimpleDateFormat]() {
    override protected def initialValue = new SimpleDateFormat("yyyyMMdd HH:mm:ss")
  }

  def parseStringOption(input: String): Option[String] = {
    input match {
      case "" => None
      case _ => Some(input)
    }
  }

  def parseDateTimeStampOption(input:String):Option[Timestamp] = {
    input match {
      case "" => None
      case "0" => None
      case inputString:String if inputString.matches("[ /:0-9]+") && inputString.length == 19 => Some(new Timestamp(timestampFormatter.get.parse(input.replace("/","")).getTime))
      case inputString:String if inputString.matches("[ \\-:0-9]+") && inputString.length == 19 => Some(new Timestamp(timestampFormatter.get.parse(input.replace("-","")).getTime))
      case inputString:String if inputString.matches("[ \\.:0-9]+") && inputString.length == 19 => Some(new Timestamp(timestampFormatter.get.parse(input.replace(".","")).getTime))
      case inputString:String if inputString.matches("[ :0-9]+") && inputString.length == 17 => Some(new Timestamp(timestampFormatter.get.parse(input).getTime))
      case inputString:String if inputString.matches("[0-9]+") && inputString.length == 8 => Some(new Timestamp(timestampFormatter.get.parse(input.concat(" 00:00:00")).getTime))
    }
  }

  private val dateFormatter = new ThreadLocal[SimpleDateFormat]() {
    override protected def initialValue = new SimpleDateFormat("yyyyMMdd")
  }

  def parseBigDecimalOption(input:String):Option[BigDecimal] = {
    input match {
      case "" => None
      case inputString:String if inputString.matches("[-,0-9]+") => Some(BigDecimal(inputString.replace(",",".")))
      case inputString:String if inputString.matches("[-.0-9]+") => Some(BigDecimal(input))
      case _ => Some(BigDecimal(0))
    }
  }

  private val longRegex = "(-?[0-9]+)[\\.,]?[0-9]*".r
  private val longRangeRegex = "([0-9]+)-([0-9]+)".r

  def parseLongRangeOption(input:String): Option[Long] = {
    input match {
      case "" => None
      case longRegex(longString) => Some(longString.toLong)
      case longRangeRegex(longString1,longString2) => Some((longString1.toLong + longString2.toLong)/2)
      case _ => None
    }
  }

  private val currencies = "\u0024\u00A2\u00A3\u00A4\u00A5\u058F\u060B\u09F2\u09F3\u09FB\u0AF1\u0BF9\u0E3F\u17DB\u20A0\u20A1\u20A2\u20A3\u20A4\u20A5\u20A6\u20A7\u20A8\u20A9\u20AA\u20AB\u20AC\u20AD\u20AE\u20AF\u20B0\u20B1\u20B2\u20B3\u20B4\u20B5\u20B6\u20B7\u20B8\u20B9\u20BA\u20BB\u20BC\u20BD\u20BE\uA838\uFDFC\uFE69\uFF04\uFFE0\uFFE1\uFFE5\uFFE6\u0081"
  private val doubleRegex = s"[$currencies]?([0-9.]+)".r
  private val doubleRangeRegex = s"[$currencies]?([0-9.]+)-[$currencies]?([0-9.]+)".r

  def parseBigDecimalRangeOption(input:String): Option[BigDecimal] = {
    input match {
      case "" => None
      case doubleRegex(bigDecimalString) => Some(BigDecimal(bigDecimalString))
      case doubleRangeRegex(bigDecimalString1,bigDecimalString2) => Some((BigDecimal(bigDecimalString1) + BigDecimal(bigDecimalString2))/2)
      case _ => None
    }
  }

  def parseBoolOption(input:String):Option[Boolean] = {
    input.toUpperCase match {
      case "" => None
      case "Y" => Some(true)
      case "N" => Some(false)
      case "A" => Some(true)
      case "D" => Some(false)
      case "X" => Some(true)
      case "1" => Some(true)
      case "0" => Some(false)
      case "TRUE" => Some(true)
      case "FALSE" => Some(false)
      case "YES" => Some(true)
      case "NO" => Some(false)
      /* Capturing strange cases from data source DEX begin*/
        case "DIRECTOR COMPRAS" => Some(true)
        case "RESPONSIBLE FOOD" => Some(true)
        case "RESPONSIBLE TEA" => Some(true)
        case "RESPONSIBLE GENERAL" => Some(true)
        case "OTHER" => Some(true)
        case _ => None
      /* Capturing strange cases from data source DEX end*/
    }
  }

  def hasValidLineLength(expectedPartCount: Int)(lineParts: Array[String])(implicit log: Logger): Boolean = {
    if (lineParts.length != expectedPartCount) {
      log.warn(
        s"Found ${lineParts.length} parts, expected $expectedPartCount in line: ${lineParts.mkString("‰")}"
      )
      false
    } else {
      true
    }
  }
}
