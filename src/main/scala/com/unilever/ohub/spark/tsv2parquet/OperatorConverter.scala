package com.unilever.ohub.spark.tsv2parquet

import java.sql.Timestamp
import java.io.InputStream

import com.unilever.ohub.spark.tsv2parquet.CustomParsers._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import StringFunctions._

import scala.io.Source

case class OperatorRecord(OPERATOR_CONCAT_ID:String,REF_OPERATOR_ID:Option[String], SOURCE:Option[String], COUNTRY_CODE:Option[String], STATUS:Option[Boolean], STATUS_ORIGINAL:Option[String], NAME:Option[String], NAME_CLEANSED:Option[String], OPR_INTEGRATION_ID:Option[String],
                          DATE_CREATED:Option[Timestamp], DATE_MODIFIED:Option[Timestamp], CHANNEL:Option[String], SUB_CHANNEL:Option[String], REGION:Option[String],
                          STREET:Option[String], STREET_CLEANSED:Option[String], HOUSENUMBER:Option[String], HOUSENUMBER_EXT:Option[String], CITY:Option[String], CITY_CLEANSED:Option[String], ZIP_CODE:Option[String], ZIP_CODE_CLEANSED:Option[String],STATE:Option[String],
                          COUNTRY:Option[String], EMAIL_ADDRESS:Option[String], PHONE_NUMBER:Option[String], MOBILE_PHONE_NUMBER:Option[String], FAX_NUMBER:Option[String],
                          OPT_OUT:Option[Boolean], EM_OPT_IN:Option[Boolean], EM_OPT_OUT:Option[Boolean], DM_OPT_IN: Option[Boolean], DM_OPT_OUT:Option[Boolean],
                          TM_OPT_IN:Option[Boolean], TM_OPT_OUT:Option[Boolean], MOB_OPT_IN: Option[Boolean], MOB_OPT_OUT:Option[Boolean],
                          FAX_OPT_IN:Option[Boolean], FAX_OPT_OUT:Option[Boolean], NR_OF_DISHES:Option[Long], NR_OF_DISHES_ORIGINAL:Option[String], NR_OF_LOCATIONS:Option[String], NR_OF_STAFF:Option[String], AVG_PRICE:Option[BigDecimal], AVG_PRICE_ORIGINAL:Option[String],
                          DAYS_OPEN:Option[Long], DAYS_OPEN_ORIGINAL:Option[String], WEEKS_CLOSED:Option[Long], WEEKS_CLOSED_ORIGINAL:Option[String], DISTRIBUTOR_NAME:Option[String], DISTRIBUTOR_CUSTOMER_NR:Option[String], OTM:Option[String],
                          OTM_REASON:Option[String], OTM_DNR:Option[Boolean], OTM_DNR_ORIGINAL:Option[String], NPS_POTENTIAL:Option[BigDecimal], NPS_POTENTIAL_ORIGINAL:Option[String], SALES_REP:Option[String], CONVENIENCE_LEVEL:Option[String],
                          PRIVATE_HOUSEHOLD:Option[Boolean], PRIVATE_HOUSEHOLD_ORIGINAL:Option[String], VAT_NUMBER:Option[String], OPEN_ON_MONDAY:Option[Boolean], OPEN_ON_MONDAY_ORIGINAL:Option[String], OPEN_ON_TUESDAY:Option[Boolean], OPEN_ON_TUESDAY_ORIGINAL:Option[String], OPEN_ON_WEDNESDAY:Option[Boolean], OPEN_ON_WEDNESDAY_ORIGINAL:Option[String],
                          OPEN_ON_THURSDAY:Option[Boolean], OPEN_ON_THURSDAY_ORIGINAL:Option[String], OPEN_ON_FRIDAY:Option[Boolean], OPEN_ON_FRIDAY_ORIGINAL:Option[String], OPEN_ON_SATURDAY:Option[Boolean], OPEN_ON_SATURDAY_ORIGINAL:Option[String], OPEN_ON_SUNDAY:Option[Boolean], OPEN_ON_SUNDAY_ORIGINAL:Option[String], CHAIN_NAME:Option[String],
                          CHAIN_ID:Option[String], KITCHEN_TYPE:Option[String])

object OperatorConverter extends App {
  if (args.length != 2) {
    println("specify INPUT_FILE OUTPUT_FILE")
    sys.exit(1)
  }

  val inputFile = args(0)
  val outputFile = args(1)

  println(s"Generating operator parquet from [$inputFile] to [$outputFile]")

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

  lazy val expectedPartCount = 59

  val startOfJob = System.currentTimeMillis()
  val inputLines:Dataset[String] = spark.read.textFile(inputFile)

  val recordsDF:DataFrame = inputLines
    .filter(line => !line.isEmpty && !line.startsWith("REF_OPERATOR_ID"))
    .map(line => line.split("‰", -1))
    .map(lineParts => {
      checkLineLength(lineParts, expectedPartCount)
      try {
        OperatorRecord(
          OPERATOR_CONCAT_ID = lineParts(2) + "~" + lineParts(1) + "~" + lineParts(0),
          REF_OPERATOR_ID = parseStringOption(lineParts(0)),
          SOURCE = parseStringOption(lineParts(1)),
          COUNTRY_CODE = parseStringOption(lineParts(2)),
          STATUS = parseBoolOption(lineParts(3)),
          STATUS_ORIGINAL = parseStringOption(lineParts(3)),
          NAME = parseStringOption(lineParts(4)),
          NAME_CLEANSED = parseStringOption(removeStrangeCharsToLowerAndTrim(lineParts(4))),
          OPR_INTEGRATION_ID = parseStringOption(lineParts(5)),
          DATE_CREATED = parseDateTimeStampOption(lineParts(6)),
          DATE_MODIFIED = parseDateTimeStampOption(lineParts(7)),
          CHANNEL = parseStringOption(lineParts(8)),
          SUB_CHANNEL = parseStringOption(lineParts(9)),
          REGION = parseStringOption(lineParts(10)),
          STREET = parseStringOption(lineParts(11)),
          STREET_CLEANSED = parseStringOption(removeStrangeCharsToLowerAndTrim(lineParts(11).concat(lineParts(12)))),
          HOUSENUMBER = parseStringOption(lineParts(12)),
          HOUSENUMBER_EXT = parseStringOption(lineParts(13)),
          CITY = parseStringOption(lineParts(14)),
          CITY_CLEANSED = parseStringOption(removeSpacesStrangeCharsAndToLower(lineParts(14))),
          ZIP_CODE = parseStringOption(lineParts(15)),
          ZIP_CODE_CLEANSED = parseStringOption(removeSpacesStrangeCharsAndToLower(lineParts(15))),
          STATE = parseStringOption(lineParts(16)),
          COUNTRY = parseStringOption(lineParts(17)),
          EMAIL_ADDRESS = parseStringOption(lineParts(18)),
          PHONE_NUMBER = parseStringOption(lineParts(19)),
          MOBILE_PHONE_NUMBER = parseStringOption(lineParts(20)),
          FAX_NUMBER = parseStringOption(lineParts(21)),
          OPT_OUT = parseBoolOption(lineParts(22)),
          EM_OPT_IN = parseBoolOption(lineParts(23)),
          EM_OPT_OUT = parseBoolOption(lineParts(24)),
          DM_OPT_IN = parseBoolOption(lineParts(25)),
          DM_OPT_OUT = parseBoolOption(lineParts(26)),
          TM_OPT_IN = parseBoolOption(lineParts(27)),
          TM_OPT_OUT = parseBoolOption(lineParts(28)),
          MOB_OPT_IN = parseBoolOption(lineParts(29)),
          MOB_OPT_OUT = parseBoolOption(lineParts(30)),
          FAX_OPT_IN = parseBoolOption(lineParts(31)),
          FAX_OPT_OUT = parseBoolOption(lineParts(32)),
          NR_OF_DISHES = parseLongRangeOption(lineParts(33)),
          NR_OF_DISHES_ORIGINAL = parseStringOption(lineParts(33)),
          NR_OF_LOCATIONS = parseStringOption(lineParts(34)),
          NR_OF_STAFF = parseStringOption(lineParts(35)),
          AVG_PRICE = parseBigDecimalRangeOption(lineParts(36)),
          AVG_PRICE_ORIGINAL = parseStringOption(lineParts(36)),
          DAYS_OPEN = parseLongRangeOption(lineParts(37)),
          DAYS_OPEN_ORIGINAL = parseStringOption(lineParts(37)),
          WEEKS_CLOSED = parseLongRangeOption(lineParts(38)),
          WEEKS_CLOSED_ORIGINAL = parseStringOption(lineParts(38)),
          DISTRIBUTOR_NAME = parseStringOption(lineParts(39)),
          DISTRIBUTOR_CUSTOMER_NR = parseStringOption(lineParts(40)),
          OTM = parseStringOption(lineParts(41)),
          OTM_REASON = parseStringOption(lineParts(42)),
          OTM_DNR = parseBoolOption(lineParts(43)),
          OTM_DNR_ORIGINAL = parseStringOption(lineParts(43)),
          NPS_POTENTIAL = parseBigDecimalRangeOption(lineParts(44)),
          NPS_POTENTIAL_ORIGINAL = parseStringOption(lineParts(44)),
          SALES_REP = parseStringOption(lineParts(45)),
          CONVENIENCE_LEVEL = parseStringOption(lineParts(46)),
          PRIVATE_HOUSEHOLD = parseBoolOption(lineParts(47)),
          PRIVATE_HOUSEHOLD_ORIGINAL = parseStringOption(lineParts(47)),
          VAT_NUMBER = parseStringOption(lineParts(48)),
          OPEN_ON_MONDAY = parseBoolOption(lineParts(49)),
          OPEN_ON_MONDAY_ORIGINAL = parseStringOption(lineParts(49)),
          OPEN_ON_TUESDAY = parseBoolOption(lineParts(50)),
          OPEN_ON_TUESDAY_ORIGINAL = parseStringOption(lineParts(50)),
          OPEN_ON_WEDNESDAY = parseBoolOption(lineParts(51)),
          OPEN_ON_WEDNESDAY_ORIGINAL = parseStringOption(lineParts(51)),
          OPEN_ON_THURSDAY = parseBoolOption(lineParts(52)),
          OPEN_ON_THURSDAY_ORIGINAL = parseStringOption(lineParts(52)),
          OPEN_ON_FRIDAY = parseBoolOption(lineParts(53)),
          OPEN_ON_FRIDAY_ORIGINAL = parseStringOption(lineParts(53)),
          OPEN_ON_SATURDAY = parseBoolOption(lineParts(54)),
          OPEN_ON_SATURDAY_ORIGINAL = parseStringOption(lineParts(54)),
          OPEN_ON_SUNDAY = parseBoolOption(lineParts(55)),
          OPEN_ON_SUNDAY_ORIGINAL = parseStringOption(lineParts(55)),
          CHAIN_NAME = parseStringOption(lineParts(56)),
          CHAIN_ID = parseStringOption(lineParts(57)),
          KITCHEN_TYPE = parseStringOption(lineParts(58))
        )
      } catch {
        case e:Exception => throw new RuntimeException(s"Exception while parsing line: ${lineParts.mkString("‰")}", e)
      }
    })
    .toDF()

  recordsDF.createOrReplaceTempView("OPERATORS")
  countryRecordsDF.createOrReplaceTempView("COUNTRIES")
  val joinedRecordsDF:DataFrame = spark.sql(
    """
      |SELECT OPR.REF_OPERATOR_ID,OPR.SOURCE,OPR.COUNTRY_CODE,OPR.STATUS,OPR.STATUS_ORIGINAL,OPR.NAME,OPR.NAME_CLEANSED,OPR.OPR_INTEGRATION_ID,OPR.DATE_CREATED,OPR.DATE_MODIFIED,OPR.CHANNEL,OPR.SUB_CHANNEL,OPR.REGION,OPR.STREET,OPR.STREET_CLEANSED,OPR.HOUSENUMBER,OPR.HOUSENUMBER_EXT,OPR.CITY,OPR.CITY_CLEANSED,OPR.ZIP_CODE,OPR.ZIP_CODE_CLEANSED,OPR.STATE,CTR.COUNTRY,OPR.EMAIL_ADDRESS,OPR.PHONE_NUMBER,OPR.MOBILE_PHONE_NUMBER,OPR.FAX_NUMBER,OPR.OPT_OUT,OPR.EM_OPT_IN,OPR.EM_OPT_OUT,OPR.DM_OPT_IN,OPR.DM_OPT_OUT,OPR.TM_OPT_IN,OPR.TM_OPT_OUT,OPR.MOB_OPT_IN,OPR.MOB_OPT_OUT,OPR.FAX_OPT_IN,OPR.FAX_OPT_OUT,OPR.NR_OF_DISHES,OPR.NR_OF_DISHES_ORIGINAL,OPR.NR_OF_LOCATIONS,OPR.NR_OF_STAFF,OPR.AVG_PRICE,OPR.AVG_PRICE_ORIGINAL,OPR.DAYS_OPEN,OPR.DAYS_OPEN_ORIGINAL,OPR.WEEKS_CLOSED,OPR.WEEKS_CLOSED_ORIGINAL,OPR.DISTRIBUTOR_NAME,OPR.DISTRIBUTOR_CUSTOMER_NR,OPR.OTM,OPR.OTM_REASON,OPR.OTM_DNR,OPR.OTM_DNR_ORIGINAL,OPR.NPS_POTENTIAL,OPR.NPS_POTENTIAL_ORIGINAL,OPR.SALES_REP,OPR.CONVENIENCE_LEVEL,OPR.PRIVATE_HOUSEHOLD,OPR.PRIVATE_HOUSEHOLD_ORIGINAL,OPR.VAT_NUMBER,OPR.OPEN_ON_MONDAY,OPR.OPEN_ON_MONDAY_ORIGINAL,OPR.OPEN_ON_TUESDAY,OPR.OPEN_ON_TUESDAY_ORIGINAL,OPR.OPEN_ON_WEDNESDAY,OPR.OPEN_ON_WEDNESDAY_ORIGINAL,OPR.OPEN_ON_THURSDAY,OPR.OPEN_ON_THURSDAY_ORIGINAL,OPR.OPEN_ON_FRIDAY,OPR.OPEN_ON_FRIDAY_ORIGINAL,OPR.OPEN_ON_SATURDAY,OPR.OPEN_ON_SATURDAY_ORIGINAL,OPR.OPEN_ON_SUNDAY,OPR.OPEN_ON_SUNDAY_ORIGINAL,OPR.CHAIN_NAME,OPR.CHAIN_ID,OPR.KITCHEN_TYPE
      |FROM OPERATORS OPR
      |LEFT JOIN COUNTRIES CTR
      | ON OPR.COUNTRY_CODE = CTR.COUNTRY_CODE
      |WHERE CTR.COUNTRY IS NOT NULL
    """.stripMargin)

  joinedRecordsDF.write.mode(Overwrite).partitionBy("COUNTRY_CODE").format("parquet").save(outputFile)

  joinedRecordsDF.printSchema()

  val count = joinedRecordsDF.count()
  println(s"Processed $count records in ${(System.currentTimeMillis - startOfJob) / 1000}s")
  println("Done")
}
