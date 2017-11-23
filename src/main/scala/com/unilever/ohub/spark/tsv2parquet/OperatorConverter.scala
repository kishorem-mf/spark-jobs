package com.unilever.ohub.spark.tsv2parquet

import java.text.SimpleDateFormat
import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode._

case class OperatorRecord(refId:String, source:String, countryCode:String, status:String, name:String, integrationId:String,
                          created:Option[Timestamp], modified:Option[Timestamp], channel:String, subChannel:String, region:String,
                          street:String, houseNumber:String, houseNumberExt:String, city:String, zipcode:String, state:String,
                          country:String, emailAddress:String, phoneNumber:String, mobilePhoneNumber:String, faxNumber:String,
                          optOut:Option[Boolean], emailOptIn:Option[Boolean], emailOptOut:Option[Boolean], directMarketingOptIn: Option[Boolean], directMarketingOptOut:Option[Boolean],
                          telemarketingOptIn:Option[Boolean], telemarketingOptOut:Option[Boolean], mobileOptIn: Option[Boolean], mobileOptOut:Option[Boolean],
                          faxOptIn:Option[Boolean], faxOptOut:Option[Boolean], nrOfDishes:String, nrOfLocations:String, nrOfStaff:String, avgPrice:String,
                          daysOpen:Option[Int], weeksClosed:Option[Int], distributorName:String, distributorCustomerNr:String, otm:String,
                          otmReason:String, otmDoNotRecalculate:String, npsPotential:String, salesRep:String, convenienceLevel:String,
                          privateHousehold:String, vatNumber:String, openOnMonday:String, openOnTuesday:String, openOnWednesday:String,
                          openOnThursday:String, openOnFriday:String, openOnSaturday:String, openOnSunday:String, chainName:String,
                          chainId:String, kitchenType:String)

object OperatorConverter extends App {
  if (args.length != 2) {
    println("specify INPUT_FILE OUTPUT_FILE")
    sys.exit(1)
  }

  val inputFile = args(0)
  val outputFile = args(1)

  println(s"Generating parquet from [$inputFile] to [$outputFile]")

  val spark = SparkSession
    .builder()
    .appName("Example Converter")
    .getOrCreate()

  import spark.implicits._

  val expectedPartCount = 59
  def checkLineLength(lineParts: Array[String]) = {
    if (lineParts.length != expectedPartCount)
      throw new RuntimeException(s"Found ${lineParts.length} parts, expected ${expectedPartCount} in line: ${lineParts.mkString("‰")}")
  }

  def parseTimeStampOption(input:String):Option[Timestamp] = {
    if (input.isEmpty) {
      None
    } else {
      try {
        Some(new Timestamp(dateFormatter.get.parse(input).getTime))
      } catch { // some ancient records use an alternate dateformat
        case _ => Some(new Timestamp(oldDateFormatter.get.parse(input).getTime))
      }
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

  val dateFormatter = new ThreadLocal[SimpleDateFormat]() {
    override protected def initialValue = new SimpleDateFormat("yyyyMMdd hh:mm:ss")
  }
  val oldDateFormatter = new ThreadLocal[SimpleDateFormat]() { // some ancient records use an alternate dateformat
    override protected def initialValue = new SimpleDateFormat("dd.MM.yyyy hh:mm")
  }

  val startOfJob = System.currentTimeMillis()

  val lines = spark.read.textFile(inputFile)

  val records = lines
    .filter(line => !line.isEmpty && !line.startsWith("REF_OPERATOR_ID"))
    .map(line => line.split("‰", -1))
    .map(lineParts => {
      checkLineLength(lineParts)
      try {
        new OperatorRecord(
          refId = lineParts(0),
          source = lineParts(1),
          countryCode = lineParts(2),
          status = lineParts(3),
          name = lineParts(4),
          integrationId = lineParts(5),
          created = parseTimeStampOption(lineParts(6)),
          modified = parseTimeStampOption(lineParts(7)),
          channel = lineParts(8),
          subChannel = lineParts(9),
          region = lineParts(10),
          street = lineParts(11),
          houseNumber = lineParts(12),
          houseNumberExt = lineParts(13),
          city = lineParts(14),
          zipcode = lineParts(15),
          state = lineParts(16),
          country = lineParts(17),
          emailAddress = lineParts(18),
          phoneNumber = lineParts(19),
          mobilePhoneNumber = lineParts(20),
          faxNumber = lineParts(21),
          optOut = parseBoolOption(lineParts(22)),
          emailOptIn = parseBoolOption(lineParts(23)),
          emailOptOut = parseBoolOption(lineParts(24)),
          directMarketingOptIn = parseBoolOption(lineParts(25)),
          directMarketingOptOut = parseBoolOption(lineParts(26)),
          telemarketingOptIn = parseBoolOption(lineParts(27)),
          telemarketingOptOut = parseBoolOption(lineParts(28)),
          mobileOptIn = parseBoolOption(lineParts(29)),
          mobileOptOut = parseBoolOption(lineParts(30)),
          faxOptIn = parseBoolOption(lineParts(31)),
          faxOptOut = parseBoolOption(lineParts(32)),
          nrOfDishes = lineParts(33),
          nrOfLocations = lineParts(34),
          nrOfStaff = lineParts(35),
          avgPrice = lineParts(36),
          daysOpen = parseIntOption(lineParts(37)),
          weeksClosed = parseIntOption(lineParts(38)),
          distributorName = lineParts(39),
          distributorCustomerNr = lineParts(40),
          otm = lineParts(41),
          otmReason = lineParts(42),
          otmDoNotRecalculate = lineParts(43),
          npsPotential = lineParts(44),
          salesRep = lineParts(44),
          convenienceLevel = lineParts(46),
          privateHousehold = lineParts(47),
          vatNumber = lineParts(48),
          openOnMonday = lineParts(49),
          openOnTuesday = lineParts(50),
          openOnWednesday = lineParts(51),
          openOnThursday = lineParts(52),
          openOnFriday = lineParts(53),
          openOnSaturday = lineParts(54),
          openOnSunday = lineParts(55),
          chainName = lineParts(56),
          chainId = lineParts(57),
          kitchenType = lineParts(58)
        )
      } catch {
        case e:Exception => throw new RuntimeException(s"Exception while parsing line: ${lineParts.mkString("‰")}", e)
      }
    })

  records.write.mode(Overwrite).partitionBy("countryCode").format("parquet").save(outputFile)

  records.printSchema()

  val count = records.count()
  println(s"Processed ${count} records in ${(System.currentTimeMillis - startOfJob) / 1000}s")
  println("Done")

}
