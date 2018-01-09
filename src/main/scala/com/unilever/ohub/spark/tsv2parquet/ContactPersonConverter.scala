package com.unilever.ohub.spark.tsv2parquet

import java.io.InputStream
import java.sql.{Date, Timestamp}

import com.unilever.ohub.spark.tsv2parquet.CustomParsers._
import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.io.Source
import StringFunctions._

case class ContactPersonRecord(REF_CONTACT_PERSON_ID:Option[String], SOURCE:Option[String], COUNTRY_CODE:Option[String], STATUS:Option[Boolean], STATUS_ORIGINAL:Option[String], REF_OPERATOR_ID:Option[String], CP_INTEGRATION_ID:Option[String],
                               DATE_CREATED:Option[Timestamp], DATE_MODIFIED:Option[Timestamp], FIRST_NAME:Option[String], FIRST_NAME_CLEANSED:Option[String], LAST_NAME:Option[String], LAST_NAME_CLEANSED:Option[String], TITLE:Option[String],
                               GENDER:Option[String], FUNCTION:Option[String], LANGUAGE_KEY:Option[String], BIRTH_DATE:Option[Timestamp], STREET:Option[String], HOUSENUMBER:Option[String],
                               HOUSENUMBER_EXT:Option[String], CITY:Option[String], ZIP_CODE:Option[String], STATE:Option[String], COUNTRY:Option[String],
                               PREFERRED_CONTACT:Option[Boolean], PREFERRED_CONTACT_ORIGINAL:Option[String], KEY_DECISION_MAKER:Option[Boolean], KEY_DECISION_MAKER_ORIGINAL:Option[String], SCM:Option[String], EMAIL_ADDRESS:Option[String],
                               PHONE_NUMBER:Option[String], MOBILE_PHONE_NUMBER:Option[String], FAX_NUMBER:Option[String], OPT_OUT:Option[Boolean], OPT_OUT_ORIGINAL:Option[String],
                               REGISTRATION_CONFIRMED: Option[Boolean], REGISTRATION_CONFIRMED_ORIGINAL:Option[String], REGISTRATION_CONFIRMED_DATE:Option[Timestamp], REGISTRATION_CONFIRMED_DATE_ORIGINAL:Option[String],
                               EM_OPT_IN:Option[Boolean], EM_OPT_IN_ORIGINAL:Option[String], EM_OPT_IN_DATE:Option[Timestamp], EM_OPT_IN_DATE_ORIGINAL:Option[String], EM_OPT_IN_CONFIRMED:Option[Boolean], EM_OPT_IN_CONFIRMED_ORIGINAL:Option[String], EM_OPT_IN_CONFIRMED_DATE:Option[Timestamp], EM_OPT_IN_CONFIRMED_DATE_ORIGINAL:Option[String],
                               EM_OPT_OUT:Option[Boolean], EM_OPT_OUT_ORIGINAL:Option[String], DM_OPT_IN: Option[Boolean], DM_OPT_IN_ORIGINAL:Option[String], DM_OPT_OUT:Option[Boolean], DM_OPT_OUT_ORIGINAL:Option[String],
                               TM_OPT_IN:Option[Boolean], TM_OPT_IN_ORIGINAL:Option[String], TM_OPT_OUT:Option[Boolean], TM_OPT_OUT_ORIGINAL:Option[String], MOB_OPT_IN:Option[Boolean], MOB_OPT_IN_ORIGINAL:Option[String], MOB_OPT_IN_DATE:Option[Timestamp], MOB_OPT_IN_DATE_ORIGINAL:Option[String],
                               MOB_OPT_IN_CONFIRMED:Option[Boolean], MOB_OPT_IN_CONFIRMED_ORIGINAL:Option[String], MOB_OPT_IN_CONFIRMED_DATE:Option[Timestamp], MOB_OPT_IN_CONFIRMED_DATE_ORIGINAL:Option[String],
                               MOB_OPT_OUT:Option[Boolean], MOB_OPT_OUT_ORIGINAL:Option[String], FAX_OPT_IN:Option[Boolean], FAX_OPT_IN_ORIGINAL:Option[String], FAX_OPT_OUT:Option[Boolean], FAX_OPT_OUT_ORIGINAL:Option[String])

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

  val inputStream:InputStream = getClass.getResourceAsStream("/country_codes.csv")
  val readSeq:Seq[String] = Source.fromInputStream(inputStream).getLines().toSeq
  val countryRecordsDF:DataFrame = spark.sparkContext.parallelize(readSeq)
    .toDS()
    .map(_.split(","))
    .map(cells => (parseStringOption(cells(6)),parseStringOption(cells(2)),parseStringOption(cells(9))))
    .filter(line => line != ("ISO3166_1_Alpha_2","official_name_en","ISO4217_currency_alphabetic_code"))
    .toDF("COUNTRY_CODE","COUNTRY","CURRENCY_CODE")

  lazy val expectedPartCount = 48

  val startOfJob = System.currentTimeMillis()

  val lines = spark.read.textFile(inputFile)

  val recordsDF:DataFrame = lines
    .filter(line => !line.isEmpty && !line.startsWith("REF_CONTACT_PERSON_ID"))
    .map(line => line.split("‰", -1))
    .map(lineParts => {
      checkLineLength(lineParts, expectedPartCount)
      lineParts.toSeq
      try {
        ContactPersonRecord(
          REF_CONTACT_PERSON_ID = parseStringOption(lineParts(0)),
          SOURCE = parseStringOption(lineParts(1)),
          COUNTRY_CODE = parseStringOption(lineParts(2)),
          STATUS = parseBoolOption(lineParts(3)),
          STATUS_ORIGINAL = parseStringOption(lineParts(3)),
          REF_OPERATOR_ID = parseStringOption(lineParts(4)),
          CP_INTEGRATION_ID = parseStringOption(lineParts(5)),
          DATE_CREATED = parseDateTimeStampOption(lineParts(6)),
          DATE_MODIFIED = parseDateTimeStampOption(lineParts(7)),
          FIRST_NAME = parseStringOption(lineParts(8)),
          FIRST_NAME_CLEANSED = parseStringOption(removeStrangeCharsToLowerAndTrim(lineParts(8))),
          LAST_NAME = parseStringOption(lineParts(9)),
          LAST_NAME_CLEANSED = parseStringOption(removeStrangeCharsToLowerAndTrim(lineParts(9))),
          TITLE = parseStringOption(lineParts(10)),
          GENDER = parseStringOption(lineParts(11)),
          FUNCTION = parseStringOption(lineParts(12)),
          LANGUAGE_KEY = parseStringOption(lineParts(13)),
          BIRTH_DATE = parseDateTimeStampOption(lineParts(14)),
          STREET = parseStringOption(lineParts(15)),
          HOUSENUMBER = parseStringOption(lineParts(16)),
          HOUSENUMBER_EXT = parseStringOption(lineParts(17)),
          CITY = parseStringOption(lineParts(18)),
          ZIP_CODE = parseStringOption(lineParts(19)),
          STATE = parseStringOption(lineParts(20)),
          COUNTRY = parseStringOption(lineParts(21)),
          PREFERRED_CONTACT = parseBoolOption(lineParts(22)),
          PREFERRED_CONTACT_ORIGINAL = parseStringOption(lineParts(22)),
          KEY_DECISION_MAKER = parseBoolOption(lineParts(23)),
          KEY_DECISION_MAKER_ORIGINAL = parseStringOption(lineParts(23)),
          SCM = parseStringOption(lineParts(24)),
          EMAIL_ADDRESS = parseStringOption(lineParts(25)),
          PHONE_NUMBER = parseStringOption(lineParts(26)),
          MOBILE_PHONE_NUMBER = parseStringOption(lineParts(27)),
          FAX_NUMBER = parseStringOption(lineParts(28)),
          OPT_OUT = parseBoolOption(lineParts(29)),
          OPT_OUT_ORIGINAL = parseStringOption(lineParts(29)),
          REGISTRATION_CONFIRMED = parseBoolOption(lineParts(30)),
          REGISTRATION_CONFIRMED_ORIGINAL = parseStringOption(lineParts(30)),
          REGISTRATION_CONFIRMED_DATE = parseDateTimeStampOption(lineParts(31)),
          REGISTRATION_CONFIRMED_DATE_ORIGINAL = parseStringOption(lineParts(31)),
          EM_OPT_IN = parseBoolOption(lineParts(32)),
          EM_OPT_IN_ORIGINAL = parseStringOption(lineParts(32)),
          EM_OPT_IN_DATE = parseDateTimeStampOption(lineParts(33)),
          EM_OPT_IN_DATE_ORIGINAL = parseStringOption(lineParts(33)),
          EM_OPT_IN_CONFIRMED = parseBoolOption(lineParts(34)),
          EM_OPT_IN_CONFIRMED_ORIGINAL = parseStringOption(lineParts(34)),
          EM_OPT_IN_CONFIRMED_DATE = parseDateTimeStampOption(lineParts(35)),
          EM_OPT_IN_CONFIRMED_DATE_ORIGINAL = parseStringOption(lineParts(35)),
          EM_OPT_OUT = parseBoolOption(lineParts(36)),
          EM_OPT_OUT_ORIGINAL = parseStringOption(lineParts(36)),
          DM_OPT_IN = parseBoolOption(lineParts(37)),
          DM_OPT_IN_ORIGINAL = parseStringOption(lineParts(37)),
          DM_OPT_OUT = parseBoolOption(lineParts(38)),
          DM_OPT_OUT_ORIGINAL = parseStringOption(lineParts(38)),
          TM_OPT_IN = parseBoolOption(lineParts(39)),
          TM_OPT_IN_ORIGINAL = parseStringOption(lineParts(39)),
          TM_OPT_OUT = parseBoolOption(lineParts(40)),
          TM_OPT_OUT_ORIGINAL = parseStringOption(lineParts(40)),
          MOB_OPT_IN = parseBoolOption(lineParts(41)),
          MOB_OPT_IN_ORIGINAL = parseStringOption(lineParts(41)),
          MOB_OPT_IN_DATE = parseDateTimeStampOption(lineParts(42)),
          MOB_OPT_IN_DATE_ORIGINAL = parseStringOption(lineParts(42)),
          MOB_OPT_IN_CONFIRMED = parseBoolOption(lineParts(43)),
          MOB_OPT_IN_CONFIRMED_ORIGINAL = parseStringOption(lineParts(43)),
          MOB_OPT_IN_CONFIRMED_DATE = parseDateTimeStampOption(lineParts(44)),
          MOB_OPT_IN_CONFIRMED_DATE_ORIGINAL = parseStringOption(lineParts(44)),
          MOB_OPT_OUT = parseBoolOption(lineParts(45)),
          MOB_OPT_OUT_ORIGINAL = parseStringOption(lineParts(45)),
          FAX_OPT_IN = parseBoolOption(lineParts(46)),
          FAX_OPT_IN_ORIGINAL = parseStringOption(lineParts(46)),
          FAX_OPT_OUT = parseBoolOption(lineParts(47)),
          FAX_OPT_OUT_ORIGINAL = parseStringOption(lineParts(47))
        )
      } catch {
        case e:Exception => throw new RuntimeException(s"Exception while parsing line: ${lineParts.mkString("‰")}", e)
      }
    })
    .toDF()

  recordsDF.createOrReplaceTempView("CONTACTPERSONS")
  countryRecordsDF.createOrReplaceTempView("COUNTRIES")
  val joinedRecordsDF:DataFrame = spark.sql(
    """
      |SELECT CPN.REF_CONTACT_PERSON_ID,CPN.SOURCE,CPN.COUNTRY_CODE,CPN.STATUS,CPN.STATUS_ORIGINAL,CPN.REF_OPERATOR_ID,CPN.CP_INTEGRATION_ID,CPN.DATE_CREATED,CPN.DATE_MODIFIED,CPN.FIRST_NAME,CPN.LAST_NAME,CPN.TITLE,CPN.GENDER,CPN.FUNCTION,CPN.LANGUAGE_KEY,CPN.BIRTH_DATE,CPN.STREET,CPN.HOUSENUMBER,CPN.HOUSENUMBER_EXT,CPN.CITY,CPN.ZIP_CODE,CPN.STATE,CTR.COUNTRY,CPN.PREFERRED_CONTACT,CPN.PREFERRED_CONTACT_ORIGINAL,CPN.KEY_DECISION_MAKER,CPN.KEY_DECISION_MAKER_ORIGINAL,CPN.SCM,CPN.EMAIL_ADDRESS,CPN.PHONE_NUMBER,CPN.MOBILE_PHONE_NUMBER,CPN.FAX_NUMBER,CPN.OPT_OUT,CPN.OPT_OUT_ORIGINAL,CPN.REGISTRATION_CONFIRMED,CPN.REGISTRATION_CONFIRMED_ORIGINAL,CPN.REGISTRATION_CONFIRMED_DATE,CPN.REGISTRATION_CONFIRMED_DATE_ORIGINAL,CPN.EM_OPT_IN,CPN.EM_OPT_IN_ORIGINAL,CPN.EM_OPT_IN_DATE,CPN.EM_OPT_IN_DATE_ORIGINAL,CPN.EM_OPT_IN_CONFIRMED,CPN.EM_OPT_IN_CONFIRMED_ORIGINAL,CPN.EM_OPT_IN_CONFIRMED_DATE,CPN.EM_OPT_IN_CONFIRMED_DATE_ORIGINAL,CPN.EM_OPT_OUT,CPN.EM_OPT_OUT_ORIGINAL,CPN.DM_OPT_IN,CPN.DM_OPT_IN_ORIGINAL,CPN.DM_OPT_OUT,CPN.DM_OPT_OUT_ORIGINAL,CPN.TM_OPT_IN,CPN.TM_OPT_IN_ORIGINAL,CPN.TM_OPT_OUT,CPN.TM_OPT_OUT_ORIGINAL,CPN.MOB_OPT_IN,CPN.MOB_OPT_IN_ORIGINAL,CPN.MOB_OPT_IN_DATE,CPN.MOB_OPT_IN_DATE_ORIGINAL,CPN.MOB_OPT_IN_CONFIRMED,CPN.MOB_OPT_IN_CONFIRMED_ORIGINAL,CPN.MOB_OPT_IN_CONFIRMED_DATE,CPN.MOB_OPT_IN_CONFIRMED_DATE_ORIGINAL,CPN.MOB_OPT_OUT,CPN.MOB_OPT_OUT_ORIGINAL,CPN.FAX_OPT_IN,CPN.FAX_OPT_IN_ORIGINAL,CPN.FAX_OPT_OUT,CPN.FAX_OPT_OUT_ORIGINAL
      |FROM CONTACTPERSONS CPN
      |LEFT JOIN COUNTRIES CTR
      | ON CPN.COUNTRY_CODE = CTR.COUNTRY_CODE
      |WHERE CTR.COUNTRY IS NOT NULL
    """.stripMargin)


  joinedRecordsDF.write.mode(Overwrite).partitionBy("COUNTRY_CODE").format("parquet").save(outputFile)

  joinedRecordsDF.printSchema()

  val count = joinedRecordsDF.count()
  println(s"Processed $count records in ${(System.currentTimeMillis - startOfJob) / 1000}s")
  println("Done")

}
