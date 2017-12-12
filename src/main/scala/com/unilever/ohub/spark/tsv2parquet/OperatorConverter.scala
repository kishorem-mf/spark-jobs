package com.unilever.ohub.spark.tsv2parquet

import java.sql.Timestamp

import com.unilever.ohub.spark.tsv2parquet.CustomParsers._
import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.SparkSession

case class OperatorRecord(REF_OPERATOR_ID:String, SOURCE:String, COUNTRY_CODE:String, STATUS:Option[Boolean], STATUS_ORIGINAL:String, NAME:String, OPR_INTEGRATION_ID:String,
                          DATE_CREATED:Option[Timestamp], DATE_MODIFIED:Option[Timestamp], CHANNEL:String, SUB_CHANNEL:String, REGION:String,
                          STREET:String, HOUSENUMBER:String, HOUSENUMBER_EXT:String, CITY:String, ZIP_CODE:String, STATE:String,
                          COUNTRY:String, EMAIL_ADDRESS:String, PHONE_NUMBER:String, MOBILE_PHONE_NUMBER:String, FAX_NUMBER:String,
                          OPT_OUT:Option[Boolean], EM_OPT_IN:Option[Boolean], EM_OPT_OUT:Option[Boolean], DM_OPT_IN: Option[Boolean], DM_OPT_OUT:Option[Boolean],
                          TM_OPT_IN:Option[Boolean], TM_OPT_OUT:Option[Boolean], MOB_OPT_IN: Option[Boolean], MOB_OPT_OUT:Option[Boolean],
                          FAX_OPT_IN:Option[Boolean], FAX_OPT_OUT:Option[Boolean], NR_OF_DISHES:Option[Long], NR_OF_DISHES_ORIGINAL:String, NR_OF_LOCATIONS:String, NR_OF_STAFF:String, AVG_PRICE:Option[BigDecimal], AVG_PRICE_ORIGINAL:String,
                          DAYS_OPEN:Option[Long], DAYS_OPEN_ORIGINAL:String, WEEKS_CLOSED:Option[Long], WEEKS_CLOSED_ORIGINAL:String, DISTRIBUTOR_NAME:String, DISTRIBUTOR_CUSTOMER_NR:String, OTM:String,
                          OTM_REASON:String, OTM_DNR:Option[Boolean], OTM_DNR_ORIGINAL:String, NPS_POTENTIAL:Option[BigDecimal], NPS_POTENTIAL_ORIGINAL:String, SALES_REP:String, CONVENIENCE_LEVEL:String,
                          PRIVATE_HOUSEHOLD:Option[Boolean], PRIVATE_HOUSEHOLD_ORIGINAL:String, VAT_NUMBER:String, OPEN_ON_MONDAY:Option[Boolean], OPEN_ON_MONDAY_ORIGINAL:String, OPEN_ON_TUESDAY:Option[Boolean], OPEN_ON_TUESDAY_ORIGINAL:String, OPEN_ON_WEDNESDAY:Option[Boolean], OPEN_ON_WEDNESDAY_ORIGINAL:String,
                          OPEN_ON_THURSDAY:Option[Boolean], OPEN_ON_THURSDAY_ORIGINAL:String, OPEN_ON_FRIDAY:Option[Boolean], OPEN_ON_FRIDAY_ORIGINAL:String, OPEN_ON_SATURDAY:Option[Boolean], OPEN_ON_SATURDAY_ORIGINAL:String, OPEN_ON_SUNDAY:Option[Boolean], OPEN_ON_SUNDAY_ORIGINAL:String, CHAIN_NAME:String,
                          CHAIN_ID:String, KITCHEN_TYPE:String)

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

  val startOfJob = System.currentTimeMillis()
  val lines = spark.read.textFile(inputFile)

  val records = lines
    .filter(line => !line.isEmpty && !line.startsWith("REF_OPERATOR_ID"))
    .map(line => line.split("‰", -1))
    .map(lineParts => {
      checkLineLength(lineParts, 59)
      try {
        OperatorRecord(
          REF_OPERATOR_ID = lineParts(0),
          SOURCE = lineParts(1),
          COUNTRY_CODE = lineParts(2),
          STATUS = parseBoolOption(lineParts(3)),
          STATUS_ORIGINAL = lineParts(3),
          NAME = lineParts(4),
          OPR_INTEGRATION_ID = lineParts(5),
          DATE_CREATED = parseDateTimeStampOption(lineParts(6)),
          DATE_MODIFIED = parseDateTimeStampOption(lineParts(7)),
          CHANNEL = lineParts(8),
          SUB_CHANNEL = lineParts(9),
          REGION = lineParts(10),
          STREET = lineParts(11),
          HOUSENUMBER = lineParts(12),
          HOUSENUMBER_EXT = lineParts(13),
          CITY = lineParts(14),
          ZIP_CODE = lineParts(15),
          STATE = lineParts(16),
          COUNTRY = lineParts(17),
          EMAIL_ADDRESS = lineParts(18),
          PHONE_NUMBER = lineParts(19),
          MOBILE_PHONE_NUMBER = lineParts(20),
          FAX_NUMBER = lineParts(21),
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
          NR_OF_DISHES_ORIGINAL = lineParts(33),
          NR_OF_LOCATIONS = lineParts(34),
          NR_OF_STAFF = lineParts(35),
          AVG_PRICE = parseBigIntegerRangeOption(lineParts(36)),
          AVG_PRICE_ORIGINAL = lineParts(36),
          DAYS_OPEN = parseLongRangeOption(lineParts(37)),
          DAYS_OPEN_ORIGINAL = lineParts(37),
          WEEKS_CLOSED = parseLongRangeOption(lineParts(38)),
          WEEKS_CLOSED_ORIGINAL = lineParts(38),
          DISTRIBUTOR_NAME = lineParts(39),
          DISTRIBUTOR_CUSTOMER_NR = lineParts(40),
          OTM = lineParts(41),
          OTM_REASON = lineParts(42),
          OTM_DNR = parseBoolOption(lineParts(43)),
          OTM_DNR_ORIGINAL = lineParts(43),
          NPS_POTENTIAL = parseBigIntegerRangeOption(lineParts(44)),
          NPS_POTENTIAL_ORIGINAL = lineParts(44),
          SALES_REP = lineParts(45),
          CONVENIENCE_LEVEL = lineParts(46),
          PRIVATE_HOUSEHOLD = parseBoolOption(lineParts(47)),
          PRIVATE_HOUSEHOLD_ORIGINAL = lineParts(47),
          VAT_NUMBER = lineParts(48),
          OPEN_ON_MONDAY = parseBoolOption(lineParts(49)),
          OPEN_ON_MONDAY_ORIGINAL = lineParts(49),
          OPEN_ON_TUESDAY = parseBoolOption(lineParts(50)),
          OPEN_ON_TUESDAY_ORIGINAL = lineParts(50),
          OPEN_ON_WEDNESDAY = parseBoolOption(lineParts(51)),
          OPEN_ON_WEDNESDAY_ORIGINAL = lineParts(51),
          OPEN_ON_THURSDAY = parseBoolOption(lineParts(52)),
          OPEN_ON_THURSDAY_ORIGINAL = lineParts(52),
          OPEN_ON_FRIDAY = parseBoolOption(lineParts(53)),
          OPEN_ON_FRIDAY_ORIGINAL = lineParts(53),
          OPEN_ON_SATURDAY = parseBoolOption(lineParts(54)),
          OPEN_ON_SATURDAY_ORIGINAL = lineParts(54),
          OPEN_ON_SUNDAY = parseBoolOption(lineParts(55)),
          OPEN_ON_SUNDAY_ORIGINAL = lineParts(55),
          CHAIN_NAME = lineParts(56),
          CHAIN_ID = lineParts(57),
          KITCHEN_TYPE = lineParts(58)
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
