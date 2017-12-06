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
      try {
        Some(new Timestamp(timestampFormatter.get.parse(input).getTime))
      } catch { // some ancient records use an alternate dateformat
        case _ => {
          try {
            Some(new Timestamp(oldTimestampFormatter1.get.parse(input).getTime))
          } catch {
            case _ => {
              try {
                Some(new Timestamp(oldTimestampFormatter2.get.parse(input).getTime))
              } catch {
                case _ => {
                  try {
                    Some(new Timestamp(dateFormatter.get.parse(input).getTime))
                  } catch {
                    case _ => None // Ok, I give up, too many date format anomalies, need to discuss this in team.
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  def parseDateOption(input:String):Option[Date] = {
    if (input.isEmpty|| input.equals("0")) {
      None
    } else {
      Some(new Date(dateFormatter.get.parse(input).getTime))
    }
  }

  def parseIntOption(input:String):Option[Int] = {
    if (input.isEmpty || input.equals("\"")) {
      None
    } else {
      try {
        Some(input.toInt)
      } catch {
        // there's some > Int.MAX values floating around in mellowmessage data, this is the least invasive workaround
        case _ => Some(input.toLong.toInt)
      }
    }
  }
  def parseBoolOption(input:String):Option[Boolean] = {
    input match {
      case "" => None
      case "Y" => Some(true)
      case "N" => Some(false)
      case _ => throw new RuntimeException(s"Unsupported boolean value: $input")
    }
  }

  def checkLineLength(lineParts: Array[String], expectedPartCount:Int):Unit = {
    if (lineParts.length != expectedPartCount)
      throw new IllegalArgumentException(s"Found ${lineParts.length} parts, expected ${expectedPartCount} in line: ${lineParts.mkString("‰")}")
  }

}
