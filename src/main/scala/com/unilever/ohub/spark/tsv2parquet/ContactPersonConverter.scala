package com.unilever.ohub.spark.tsv2parquet

import java.sql.{Date, Timestamp}

import com.unilever.ohub.spark.tsv2parquet.CustomParsers._
import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.SparkSession

case class ContactPersonRecord(refId:String, source:String, countryCode:String, status:String, refOperatorId:String, integrationId:String,
                               created:Option[Timestamp], modified:Option[Timestamp], firstName:String, lasteName:String, title:String,
                               gender:String, function:String, language:String, birthDate:Option[Date], street:String, houseNumber:String,
                               houseNumberExt:String, city:String, zipcode:String, state:String, country:String,
                               preferredContact:Option[Boolean], keyDecisionMaker:Option[Boolean], scm:String, emailAddress:String,
                               phoneNumber:String, mobilePhoneNumber:String, faxNumber:String, optOut:Option[Boolean],
                               registrationConfirmed: Option[Boolean], registrationConfirmedDate:Option[Timestamp],
                               emailOptIn:Option[Boolean], emailOptInDate:Option[Timestamp], emailOptInConfirmed:Option[Boolean], emailOptInConfirmedDate:Option[Timestamp],
                               emailOptOut:Option[Boolean], directMarketingOptIn: Option[Boolean], directMarketingOptOut:Option[Boolean],
                               telemarketingOptIn:Option[Boolean], telemarketingOptOut:Option[Boolean], mobileOptIn:Option[Boolean], mobileOptInDate:Option[Timestamp],
                               mobileOptInConfirmed:Option[Boolean], mobileOptInConfirmedDate:Option[Timestamp],
                               mobileOptOut:Option[Boolean], faxOptIn:Option[Boolean], faxOptOut:Option[Boolean])

object ContactPersonConverter extends App {
  if (args.length != 2) {
    println("specify INPUT_FILE OUTPUT_FILE")
    sys.exit(1)
  }

  val inputFile = args(0)
  val outputFile = args(1)

  println(s"Generating parquet from [$inputFile] to [$outputFile]")

  val spark = SparkSession
    .builder()
    .appName(this.getClass.getSimpleName)
    .getOrCreate()

  import spark.implicits._

  val expectedPartCount = 48

  val startOfJob = System.currentTimeMillis()

  val lines = spark.read.textFile(inputFile)

  val records = lines
    .filter(line => !line.isEmpty && !line.startsWith("REF_CONTACT_PERSON_ID"))
    .map(line => line.split("‰", -1))
    .map(lineParts => {
      checkLineLength(lineParts, expectedPartCount)
      lineParts.toSeq
      try {
        new ContactPersonRecord(
          refId = lineParts(0),
          source = lineParts(1),
          countryCode = lineParts(2),
          status = lineParts(3),
          refOperatorId = lineParts(4),
          integrationId = lineParts(5),
          created = parseTimeStampOption(lineParts(6)),
          modified = parseTimeStampOption(lineParts(7)),
          firstName = lineParts(8),
          lasteName = lineParts(9),
          title = lineParts(10),
          gender = lineParts(11),
          function = lineParts(12),
          language = lineParts(13),
          birthDate = parseDateOption(lineParts(14)),
          street = lineParts(15),
          houseNumber = lineParts(16),
          houseNumberExt = lineParts(17),
          city = lineParts(18),
          zipcode = lineParts(19),
          state = lineParts(20),
          country = lineParts(21),
          preferredContact = parseBoolOption(lineParts(22)),
          keyDecisionMaker = parseBoolOption(lineParts(23)),
          scm = lineParts(24),
          emailAddress = lineParts(25),
          phoneNumber = lineParts(26),
          mobilePhoneNumber = lineParts(27),
          faxNumber = lineParts(28),
          optOut = parseBoolOption(lineParts(29)),
          registrationConfirmed = parseBoolOption(lineParts(30)),
          registrationConfirmedDate = parseTimeStampOption(lineParts(31)),
          emailOptIn = parseBoolOption(lineParts(32)),
          emailOptInDate = parseTimeStampOption(lineParts(33)),
          emailOptInConfirmed = parseBoolOption(lineParts(34)),
          emailOptInConfirmedDate = parseTimeStampOption(lineParts(35)),
          emailOptOut = parseBoolOption(lineParts(36)),
          directMarketingOptIn = parseBoolOption(lineParts(37)),
          directMarketingOptOut = parseBoolOption(lineParts(38)),
          telemarketingOptIn = parseBoolOption(lineParts(39)),
          telemarketingOptOut = parseBoolOption(lineParts(40)),
          mobileOptIn = parseBoolOption(lineParts(41)),
          mobileOptInDate = parseTimeStampOption(lineParts(42)),
          mobileOptInConfirmed = parseBoolOption(lineParts(43)),
          mobileOptInConfirmedDate = parseTimeStampOption(lineParts(44)),
          mobileOptOut = parseBoolOption(lineParts(45)),
          faxOptIn = parseBoolOption(lineParts(46)),
          faxOptOut = parseBoolOption(lineParts(47))
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
