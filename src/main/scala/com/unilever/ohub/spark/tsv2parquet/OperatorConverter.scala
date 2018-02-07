package com.unilever.ohub.spark.tsv2parquet

import java.sql.Timestamp

import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.tsv2parquet.CustomParsers.{ parseBigDecimalRangeOption, parseBoolOption, parseDateTimeStampOption, parseLongRangeOption, parseStringOption }
import com.unilever.ohub.spark.generic.StringFunctions.{ removeSpacesStrangeCharsAndToLower, removeStrangeCharsToLowerAndTrim }
import com.unilever.ohub.spark.storage.{ CountryRecord, Storage }
import org.apache.spark.sql.{ Dataset, SparkSession }

case class OperatorRecord(
  OPERATOR_CONCAT_ID: String, REF_OPERATOR_ID: Option[String], SOURCE: Option[String],
  COUNTRY_CODE: Option[String], STATUS: Option[Boolean], STATUS_ORIGINAL: Option[String],
  NAME: Option[String], NAME_CLEANSED: Option[String], OPR_INTEGRATION_ID: Option[String],
  DATE_CREATED: Option[Timestamp], DATE_MODIFIED: Option[Timestamp], CHANNEL: Option[String],
  SUB_CHANNEL: Option[String], REGION: Option[String],
  STREET: Option[String], STREET_CLEANSED: Option[String], HOUSENUMBER: Option[String],
  HOUSENUMBER_EXT: Option[String], CITY: Option[String], CITY_CLEANSED: Option[String],
  ZIP_CODE: Option[String], ZIP_CODE_CLEANSED: Option[String], STATE: Option[String],
  COUNTRY: Option[String], EMAIL_ADDRESS: Option[String], PHONE_NUMBER: Option[String],
  MOBILE_PHONE_NUMBER: Option[String], FAX_NUMBER: Option[String],
  OPT_OUT: Option[Boolean], EM_OPT_IN: Option[Boolean], EM_OPT_OUT: Option[Boolean],
  DM_OPT_IN: Option[Boolean], DM_OPT_OUT: Option[Boolean],
  TM_OPT_IN: Option[Boolean], TM_OPT_OUT: Option[Boolean], MOB_OPT_IN: Option[Boolean],
  MOB_OPT_OUT: Option[Boolean],
  FAX_OPT_IN: Option[Boolean], FAX_OPT_OUT: Option[Boolean], NR_OF_DISHES: Option[Long],
  NR_OF_DISHES_ORIGINAL: Option[String], NR_OF_LOCATIONS: Option[String], NR_OF_STAFF: Option[String],
  AVG_PRICE: Option[BigDecimal], AVG_PRICE_ORIGINAL: Option[String],
  DAYS_OPEN: Option[Long], DAYS_OPEN_ORIGINAL: Option[String], WEEKS_CLOSED: Option[Long],
  WEEKS_CLOSED_ORIGINAL: Option[String], DISTRIBUTOR_NAME: Option[String],
  DISTRIBUTOR_CUSTOMER_NR: Option[String], OTM: Option[String],
  OTM_REASON: Option[String], OTM_DNR: Option[Boolean], OTM_DNR_ORIGINAL: Option[String],
  NPS_POTENTIAL: Option[BigDecimal], NPS_POTENTIAL_ORIGINAL: Option[String], SALES_REP: Option[String],
  CONVENIENCE_LEVEL: Option[String],
  PRIVATE_HOUSEHOLD: Option[Boolean], PRIVATE_HOUSEHOLD_ORIGINAL: Option[String],
  VAT_NUMBER: Option[String], OPEN_ON_MONDAY: Option[Boolean], OPEN_ON_MONDAY_ORIGINAL: Option[String],
  OPEN_ON_TUESDAY: Option[Boolean], OPEN_ON_TUESDAY_ORIGINAL: Option[String],
  OPEN_ON_WEDNESDAY: Option[Boolean], OPEN_ON_WEDNESDAY_ORIGINAL: Option[String],
  OPEN_ON_THURSDAY: Option[Boolean], OPEN_ON_THURSDAY_ORIGINAL: Option[String],
  OPEN_ON_FRIDAY: Option[Boolean], OPEN_ON_FRIDAY_ORIGINAL: Option[String],
  OPEN_ON_SATURDAY: Option[Boolean], OPEN_ON_SATURDAY_ORIGINAL: Option[String],
  OPEN_ON_SUNDAY: Option[Boolean], OPEN_ON_SUNDAY_ORIGINAL: Option[String], CHAIN_NAME: Option[String],
  CHAIN_ID: Option[String], KITCHEN_TYPE: Option[String]
)

object OperatorConverter extends SparkJob  {
  private def linePartsToOperatorRecord(lineParts: Array[String]): OperatorRecord = {
    try {
      OperatorRecord(
        OPERATOR_CONCAT_ID = s"${lineParts(2)}~${lineParts(1)}~${lineParts(0)}",
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
        STREET_CLEANSED = parseStringOption(removeStrangeCharsToLowerAndTrim(lineParts(11).concat
        (lineParts(12)))),
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
      case e: Exception =>
        throw new RuntimeException(s"Exception while parsing line: ${lineParts.mkString("‰")}", e)
    }
  }

  def transform(
    spark: SparkSession,
    operatorRecords: Dataset[OperatorRecord],
    countryRecords: Dataset[CountryRecord]
  ): Dataset[OperatorRecord] = {
    import spark.implicits._

    operatorRecords
      .joinWith(
        countryRecords,
        countryRecords("COUNTRY").isNotNull and
          operatorRecords("COUNTRY_CODE") === countryRecords("COUNTRY_CODE"),
        "left"
      )
      .map {
        case (operatorRecord, countryRecord) => operatorRecord.copy(
          COUNTRY = Some(countryRecord.COUNTRY),
          COUNTRY_CODE = Some(countryRecord.COUNTRY_CODE)
        )
      }
  }

  override val neededFilePaths = Array("INPUT_FILE", "OUTPUT_FILE")

  override def run(spark: SparkSession, filePaths: Product, storage: Storage): Unit = {
    import spark.implicits._

    val (inputFile: String, outputFile: String) = filePaths

    val hasValidLineLength = CustomParsers.hasValidLineLength(59) _

    val operatorRecords = storage
      .readFromCSV[String](inputFile)
      .map(_.split("‰", -1))
      .filter(hasValidLineLength)
      .map(linePartsToOperatorRecord)

    val countryRecords = storage.countries

    val transformed = transform(spark, operatorRecords, countryRecords)

    storage.writeToParquet(transformed, outputFile, partitionBy = "COUNTRY_CODE")
  }
}
