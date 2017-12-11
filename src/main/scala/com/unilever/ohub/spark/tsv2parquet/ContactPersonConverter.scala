package com.unilever.ohub.spark.tsv2parquet

import java.sql.{Date, Timestamp}

import com.unilever.ohub.spark.tsv2parquet.CustomParsers._
import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.SparkSession

case class ContactPersonRecord(REF_CONTACT_PERSON_ID:String, SOURCE:String, COUNTRY_CODE:String, STATUS:Option[Boolean],STATUS_ORIGINAL:String, REF_OPERATOR_ID:String, CP_INTEGRATION_ID:String,
                               DATE_CREATED:Option[Timestamp], DATE_MODIFIED:Option[Timestamp], FIRST_NAME:String, LAST_NAME:String, TITLE:String,
                               GENDER:String, FUNCTION:String, LANGUAGE_KEY:String, BIRTH_DATE:Option[Date], STREET:String, HOUSENUMBER:String,
                               HOUSENUMBER_EXT:String, CITY:String, ZIP_CODE:String, STATE:String, COUNTRY:String,
                               PREFERRED_CONTACT:Option[Boolean], PREFERRED_CONTACT_ORIGINAL:String, KEY_DECISION_MAKER:Option[Boolean], KEY_DECISION_MAKER_ORIGINAL:String, SCM:String, EMAIL_ADDRESS:String,
                               PHONE_NUMBER:String, MOBILE_PHONE_NUMBER:String, FAX_NUMBER:String, OPT_OUT:Option[Boolean], OPT_OUT_ORIGINAL:String,
                               REGISTRATION_CONFIRMED: Option[Boolean], REGISTRATION_CONFIRMED_ORIGINAL:String, REGISTRATION_CONFIRMED_DATE:Option[Timestamp], REGISTRATION_CONFIRMED_DATE_ORIGINAL:String,
                               EM_OPT_IN:Option[Boolean], EM_OPT_IN_ORIGINAL:String, EM_OPT_IN_DATE:Option[Timestamp], EM_OPT_IN_DATE_ORIGINAL:String, EM_OPT_IN_CONFIRMED:Option[Boolean], EM_OPT_IN_CONFIRMED_ORIGINAL:String, EM_OPT_IN_CONFIRMED_DATE:Option[Timestamp], EM_OPT_IN_CONFIRMED_DATE_ORIGINAL:String,
                               EM_OPT_OUT:Option[Boolean], EM_OPT_OUT_ORIGINAL:String, DM_OPT_IN: Option[Boolean], DM_OPT_IN_ORIGINAL:String, DM_OPT_OUT:Option[Boolean], DM_OPT_OUT_ORIGINAL:String,
                               TM_OPT_IN:Option[Boolean], TM_OPT_IN_ORIGINAL:String, TM_OPT_OUT:Option[Boolean], TM_OPT_OUT_ORIGINAL:String, MOB_OPT_IN:Option[Boolean], MOB_OPT_IN_ORIGINAL:String, MOB_OPT_IN_DATE:Option[Timestamp], MOB_OPT_IN_DATE_ORIGINAL:String,
                               MOB_OPT_IN_CONFIRMED:Option[Boolean], MOB_OPT_IN_CONFIRMED_ORIGINAL:String, MOB_OPT_IN_CONFIRMED_DATE:Option[Timestamp], MOB_OPT_IN_CONFIRMED_DATE_ORIGINAL:String,
                               MOB_OPT_OUT:Option[Boolean], MOB_OPT_OUT_ORIGINAL:String, FAX_OPT_IN:Option[Boolean], FAX_OPT_IN_ORIGINAL:String, FAX_OPT_OUT:Option[Boolean], FAX_OPT_OUT_ORIGINAL:String)

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
        ContactPersonRecord(
          REF_CONTACT_PERSON_ID = lineParts(0),
          SOURCE = lineParts(1),
          COUNTRY_CODE = lineParts(2),
          STATUS = parseBoolOption(lineParts(3)),
          STATUS_ORIGINAL = lineParts(3),
          REF_OPERATOR_ID = lineParts(4),
          CP_INTEGRATION_ID = lineParts(5),
          DATE_CREATED = parseTimeStampOption(lineParts(6)),
          DATE_MODIFIED = parseTimeStampOption(lineParts(7)),
          FIRST_NAME = lineParts(8),
          LAST_NAME = lineParts(9),
          TITLE = lineParts(10),
          GENDER = lineParts(11),
          FUNCTION = lineParts(12),
          LANGUAGE_KEY = lineParts(13),
          BIRTH_DATE = parseDateOption(lineParts(14)),
          STREET = lineParts(15),
          HOUSENUMBER = lineParts(16),
          HOUSENUMBER_EXT = lineParts(17),
          CITY = lineParts(18),
          ZIP_CODE = lineParts(19),
          STATE = lineParts(20),
          COUNTRY = lineParts(21),
          PREFERRED_CONTACT = parseBoolOption(lineParts(22)),
          PREFERRED_CONTACT_ORIGINAL = lineParts(22),
          KEY_DECISION_MAKER = parseBoolOption(lineParts(23)),
          KEY_DECISION_MAKER_ORIGINAL = lineParts(23),
          SCM = lineParts(24),
          EMAIL_ADDRESS = lineParts(25),
          PHONE_NUMBER = lineParts(26),
          MOBILE_PHONE_NUMBER = lineParts(27),
          FAX_NUMBER = lineParts(28),
          OPT_OUT = parseBoolOption(lineParts(29)),
          OPT_OUT_ORIGINAL = lineParts(29),
          REGISTRATION_CONFIRMED = parseBoolOption(lineParts(30)),
          REGISTRATION_CONFIRMED_ORIGINAL = lineParts(30),
          REGISTRATION_CONFIRMED_DATE = parseTimeStampOption(lineParts(31)),
          REGISTRATION_CONFIRMED_DATE_ORIGINAL = lineParts(31),
          EM_OPT_IN = parseBoolOption(lineParts(32)),
          EM_OPT_IN_ORIGINAL = lineParts(32),
          EM_OPT_IN_DATE = parseTimeStampOption(lineParts(33)),
          EM_OPT_IN_DATE_ORIGINAL = lineParts(33),
          EM_OPT_IN_CONFIRMED = parseBoolOption(lineParts(34)),
          EM_OPT_IN_CONFIRMED_ORIGINAL = lineParts(34),
          EM_OPT_IN_CONFIRMED_DATE = parseTimeStampOption(lineParts(35)),
          EM_OPT_IN_CONFIRMED_DATE_ORIGINAL = lineParts(35),
          EM_OPT_OUT = parseBoolOption(lineParts(36)),
          EM_OPT_OUT_ORIGINAL = lineParts(36),
          DM_OPT_IN = parseBoolOption(lineParts(37)),
          DM_OPT_IN_ORIGINAL = lineParts(37),
          DM_OPT_OUT = parseBoolOption(lineParts(38)),
          DM_OPT_OUT_ORIGINAL = lineParts(38),
          TM_OPT_IN = parseBoolOption(lineParts(39)),
          TM_OPT_IN_ORIGINAL = lineParts(39),
          TM_OPT_OUT = parseBoolOption(lineParts(40)),
          TM_OPT_OUT_ORIGINAL = lineParts(40),
          MOB_OPT_IN = parseBoolOption(lineParts(41)),
          MOB_OPT_IN_ORIGINAL = lineParts(41),
          MOB_OPT_IN_DATE = parseTimeStampOption(lineParts(42)),
          MOB_OPT_IN_DATE_ORIGINAL = lineParts(42),
          MOB_OPT_IN_CONFIRMED = parseBoolOption(lineParts(43)),
          MOB_OPT_IN_CONFIRMED_ORIGINAL = lineParts(43),
          MOB_OPT_IN_CONFIRMED_DATE = parseTimeStampOption(lineParts(44)),
          MOB_OPT_IN_CONFIRMED_DATE_ORIGINAL = lineParts(44),
          MOB_OPT_OUT = parseBoolOption(lineParts(45)),
          MOB_OPT_OUT_ORIGINAL = lineParts(45),
          FAX_OPT_IN = parseBoolOption(lineParts(46)),
          FAX_OPT_IN_ORIGINAL = lineParts(46),
          FAX_OPT_OUT = parseBoolOption(lineParts(47)),
          FAX_OPT_OUT_ORIGINAL = lineParts(47)
        )
      } catch {
        case e:Exception => throw new RuntimeException(s"Exception while parsing line: ${lineParts.mkString("‰")}", e)
      }
    })

  records.write.mode(Overwrite).partitionBy("COUNTRY_CODE").format("parquet").save(outputFile)

  records.printSchema()

  val count = records.count()
  println(s"Processed $count records in ${(System.currentTimeMillis - startOfJob) / 1000}s")
  println("Done")

}
