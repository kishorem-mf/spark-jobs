package com.unilever.ohub.spark.tsv2parquet

import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat

object CustomParsers {

  val timestampFormatter = new ThreadLocal[SimpleDateFormat]() {
    override protected def initialValue = new SimpleDateFormat("yyyyMMdd HH:mm:ss")
  }

  val oldTimestampFormatter1 = new ThreadLocal[SimpleDateFormat]() { // some ancient records use an alternate dateformat
    override protected def initialValue = new SimpleDateFormat("dd.MM.yyyy HH:mm")
  }

  val oldTimestampFormatter2 = new ThreadLocal[SimpleDateFormat]() { // some ancient records use an alternate dateformat
    override protected def initialValue = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss")
  }

  val dateFormatter = new ThreadLocal[SimpleDateFormat]() {
    override protected def initialValue = new SimpleDateFormat("yyyyMMdd")
  }

  // TODO find something better then nested try statements for this
  def parseTimeStampOption(input:String):Option[Timestamp] = {
    if (input.isEmpty) {
      None
    } else {
        Some(new Timestamp(timestampFormatter.get.parse(input).getTime))
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

  def parseDateOption(input:String):Option[Date] = {
    if (input.isEmpty|| input.equals("0")) {
      None
    } else {
      Some(new Date(dateFormatter.get.parse(input).getTime))
    }
  }

  def parseIntOption(input:String):Option[Long] = {
    if (input.isEmpty || input.equals("\"")) {
      None
    } else {
      try {
        Some(input.toLong)
      } catch {
        case _: NumberFormatException => Some(input.toLong)
      }
    }
  }
  def parseBoolOption(input:String):Option[Boolean] = {
    input match {
      case "" => None
      case "Y" => Some(true)
      case "N" => Some(false)
      case "A" => Some(true)
      case "D" => Some(false)
      case _ => throw new RuntimeException(s"Unsupported boolean value: $input")
    }
  }

  def checkLineLength(lineParts: Array[String], expectedPartCount:Int) = {
    if (lineParts.length != expectedPartCount)
      throw new RuntimeException(s"Found ${lineParts.length} parts, expected ${expectedPartCount} in line: ${lineParts.mkString("‰")}")
  }

}
