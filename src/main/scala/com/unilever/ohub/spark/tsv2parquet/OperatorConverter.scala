package com.unilever.ohub.spark.tsv2parquet

import java.text.SimpleDateFormat
import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode._

case class OperatorRecord(refId:String, source:String, countryCode:String, status:String, name:String, integrationId:String,
                          created:Timestamp, modified:Timestamp, channel:String, subChannel:String, region:String, street:String,
                          houseNumber:Option[Int], houseNumberExt:String, city:String, zipcode:String, state:String, country:String,
                          emailAddress:String, phoneNumber:String, mobilePhoneNumber:String, faxNumber:String, optOut:String,
                          emailOptIn:String, emailOptOut:String, directMarketingOptIn: String, directMarketingOptOut:String,
                          telemarketingOptIn:String, telemarketingOptOut:String, mobileOptIn: String, mobileOptOut:String,
                          faxOptIn:String, faxOptOut:String, nrOfDishes:String, nrOfLocations:String, nrOfStaff:String, avgPrice:String,
                          daysOpen:String, weeksClosed:String, distributorName:String, distributorCustomerNr:String, otm:String,
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
      throw new RuntimeException(s"Found ${lineParts.length} parts, expected ${expectedPartCount} in line: ${lineParts.toSeq}")
  }

  def parseTimeStamp(input:String):Timestamp = new Timestamp(dateFormatter.get.parse(input).getTime)
  def parseIntOption(input:String):Option[Int] = if (input.isEmpty) None else Some(input.toInt)

  val dateFormatter = new ThreadLocal[SimpleDateFormat]() {
    override protected def initialValue = new SimpleDateFormat("yyyyMMdd hh:mm:ss")
  }

  val lines = spark.read.textFile(inputFile)

  val records = lines
    .filter(line => !line.isEmpty && !line.startsWith("REF_OPERATOR_ID"))
    .map(line => line.split("‰", -1))
    .map(lineParts => {
      checkLineLength(lineParts)
      new OperatorRecord(
        refId = lineParts(0),
        source = lineParts(1),
        countryCode = lineParts(2),
        status = lineParts(3),
        name = lineParts(4),
        integrationId = lineParts(5),
        created = parseTimeStamp(lineParts(6)),
        modified = parseTimeStamp(lineParts(7)),
        channel = lineParts(8),
        subChannel = lineParts(9),
        region = lineParts(10),
        street = lineParts(11),
        houseNumber = parseIntOption(lineParts(12)),
        houseNumberExt = lineParts(13),
        city = lineParts(14),
        zipcode = lineParts(15),
        state = lineParts(16),
        country = lineParts(17),
        emailAddress = lineParts(18),
        phoneNumber = lineParts(19),
        mobilePhoneNumber = lineParts(20),
        faxNumber = lineParts(21),
        optOut = lineParts(22),
        emailOptIn = lineParts(23),
        emailOptOut = lineParts(24),
        directMarketingOptIn = lineParts(25),
        directMarketingOptOut = lineParts(26),
        telemarketingOptIn = lineParts(27),
        telemarketingOptOut = lineParts(28),
        mobileOptIn = lineParts(29),
        mobileOptOut = lineParts(30),
        faxOptIn = lineParts(31),
        faxOptOut = lineParts(32),
        nrOfDishes = lineParts(33),
        nrOfLocations = lineParts(34),
        nrOfStaff = lineParts(35),
        avgPrice = lineParts(36),
        daysOpen = lineParts(37),
        weeksClosed = lineParts(38),
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
    })

  records.printSchema()

  records.write.mode(Overwrite).format("parquet").save(outputFile)

  spark.read.parquet(outputFile)
    .as[OperatorRecord]
    .show(1000, truncate = false)

  println("Done")

}
