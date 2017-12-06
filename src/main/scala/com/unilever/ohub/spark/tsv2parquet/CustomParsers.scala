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

  def parseBoolOption(input:String):Option[Boolean] = {
    input match {
      case "" => None
      case "Y" => Some(true)
      case "N" => Some(false)
      case "A" => Some(true)
      case "D" => Some(false)
      case _ => throw new IllegalArgumentException(s"Unsupported boolean value: $input")
    }
  }

  def checkLineLength(lineParts: Array[String], expectedPartCount:Int):Unit = {
    if (lineParts.length != expectedPartCount)
      throw new IllegalArgumentException(s"Found ${lineParts.length} parts, expected ${expectedPartCount} in line: ${lineParts.mkString("‰")}")
  }

}
