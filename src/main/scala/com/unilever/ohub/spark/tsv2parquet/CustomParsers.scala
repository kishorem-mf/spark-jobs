package com.unilever.ohub.spark.tsv2parquet

import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat

object CustomParsers {

//  TODO Complete tests in CustomParsersSpec for new parsers

  private val timestampFormatter = new ThreadLocal[SimpleDateFormat]() {
    override protected def initialValue = new SimpleDateFormat("yyyyMMdd HH:mm:ss")
  }

  def parseStringOption(input:String):Option[String] = {
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

  private val currencies = "$₠₡₢₣₤₥₦₧₨₩₪₫€₭₮₯₰₱₲₳₴₵₶₷₸₹"
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

  def checkLineLength(lineParts: Array[String], expectedPartCount:Int):Unit = {
    if (lineParts.length != expectedPartCount)
      throw new IllegalArgumentException(s"Found ${lineParts.length} parts, expected $expectedPartCount in line: ${lineParts.mkString("‰")}")
  }

}
