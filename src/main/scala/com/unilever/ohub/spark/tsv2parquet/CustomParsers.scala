package com.unilever.ohub.spark.tsv2parquet

import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat

object CustomParsers {

  private val timestampFormatter = new ThreadLocal[SimpleDateFormat]() {
    override protected def initialValue = new SimpleDateFormat("yyyyMMdd HH:mm:ss")
  }

  def parseTimeStampOption(input:String):Option[Timestamp] = {
    if (input.isEmpty) {
      None
    } else {
        Some(new Timestamp(timestampFormatter.get.parse(input).getTime))
    }
  }

  def parseDateTimeStampOption(input:String):Option[Timestamp] = {
    input match {
      case "" => None
      case inputString:String if inputString.matches("[ :0-9]+") && inputString.length == 19 => Some(new Timestamp(timestampFormatter.get.parse(input.replace("-","").replace("/","").replace(".","")).getTime))
      case inputString:String if inputString.matches("[ :0-9]+") && inputString.length == 17 => Some(new Timestamp(timestampFormatter.get.parse(input).getTime))
      case inputString:String if inputString.matches("[0-9]+") && inputString.length == 8 => Some(new Timestamp(timestampFormatter.get.parse(input.concat(" 00:00:00")).getTime))
      case _ => None
    }
  }

  private val dateFormatter = new ThreadLocal[SimpleDateFormat]() {
    override protected def initialValue = new SimpleDateFormat("yyyyMMdd")
  }

  // TODO See if we can get rid of the "0" case after isempty
  def parseDateOption(input:String):Option[Date] = {
    if (input.isEmpty|| input.equals("0")) {
      None
    } else {
      Some(new Date(dateFormatter.get.parse(input).getTime))
    }
  }

  // TODO See if we can get rid of the "\"" case after isempty
  def parseLongOption(input:String):Option[Long] = {
    if (input.isEmpty || input.equals("\"")) {
      None
    } else {
      Some(input.toLong)
    }
  }

  def parseDecimalOption(input:String):Option[BigDecimal] = {
    if (input.isEmpty) {
      None
    } else {
      Some(BigDecimal(input))
    }
  }

  private val longRegex = "([0-9]+)".r
  private val longRangeRegex = "([0-9]+)-([0-9]+)".r

  def parseLongRangeOption(input:String): Option[Long] = {
    input match {
      case "" => None
      case longRegex(longString) => Some(longString.toLong)
      case longRangeRegex(longString1,longString2) => Some((longString1.toLong + longString2.toLong)/2)
      case _ => None
    }
  }

  private val doubleRegex = "([0-9.]+)".r
  private val doubleRangeRegex = "([0-9.]+)-([0-9.]+)".r

  // TODO Find something more efficient for that replaceAll
  def parseDoubleRangeOption(input:String): Option[Double] = {
    input.replaceAll("[\u20A0\u20A1\u20A2\u20A3\u20A4\u20A5\u20A6\u20A7\u20A8\u20A9\u20AA\u20AB\u20AC\u20AD\u20AE\u20AF\u20B0\u20B1\u20B2\u20B3\u20B4\u20B5\u20B6\u20B7\u20B8\u20B9]","") match {
      case "" => None
      case doubleRegex(doubleString) => Some(doubleString.toDouble)
      case doubleRangeRegex(doubleString1,doubleString2) => Some((doubleString1.toDouble + doubleString2.toDouble)/2)
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
      /* Capturing strange cases from data source DEX end*/
      case _ => Some(true)
    }
  }

  def checkLineLength(lineParts: Array[String], expectedPartCount:Int):Unit = {
    if (lineParts.length != expectedPartCount)
      throw new IllegalArgumentException(s"Found ${lineParts.length} parts, expected $expectedPartCount in line: ${lineParts.mkString("‰")}")
  }

}
