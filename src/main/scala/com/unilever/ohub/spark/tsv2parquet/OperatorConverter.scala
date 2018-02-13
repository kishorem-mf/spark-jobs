package com.unilever.ohub.spark.tsv2parquet

import java.sql.Timestamp

import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.generic.StringFunctions.{ removeSpacesStrangeCharsAndToLower, removeStrangeCharsToLowerAndTrim }
import com.unilever.ohub.spark.sql.JoinType
import com.unilever.ohub.spark.storage.{ CountryRecord, Storage }
import CustomParsers.Implicits._
import org.apache.spark.sql.{ Dataset, Row, SparkSession }

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
  private val csvColumnSeparator = "‰"

  private def rowToOperatorRecord(row: Row): OperatorRecord = {
    val refOperatorId = row.parseStringOption(0)
    val source = row.parseStringOption(1)
    val countryCode = row.parseStringOption(2)
    val concatId = s"${countryCode.getOrElse("")}~${source.getOrElse("")}~${refOperatorId.getOrElse("")}"

    OperatorRecord(
      OPERATOR_CONCAT_ID = concatId,
      REF_OPERATOR_ID = refOperatorId,
      SOURCE = source,
      COUNTRY_CODE = countryCode,
      STATUS = row.parseBooleanOption(3),
      STATUS_ORIGINAL = row.parseStringOption(3),
      NAME = row.parseStringOption(4),
      NAME_CLEANSED = row.parseStringOption(4).map(removeStrangeCharsToLowerAndTrim),
      OPR_INTEGRATION_ID = row.parseStringOption(5),
      DATE_CREATED = row.parseDateTimeStampOption(6),
      DATE_MODIFIED = row.parseDateTimeStampOption(7),
      CHANNEL = row.parseStringOption(8),
      SUB_CHANNEL = row.parseStringOption(9),
      REGION = row.parseStringOption(10),
      STREET = row.parseStringOption(11),
      STREET_CLEANSED = row
        .parseStringOption(11)
        .map(removeStrangeCharsToLowerAndTrim)
        .map(_ + row.parseStringOption(12).getOrElse("")),
      HOUSENUMBER = row.parseStringOption(12),
      HOUSENUMBER_EXT = row.parseStringOption(123),
      CITY = row.parseStringOption(14),
      CITY_CLEANSED = row.parseStringOption(14).map(removeSpacesStrangeCharsAndToLower),
      ZIP_CODE = row.parseStringOption(15),
      ZIP_CODE_CLEANSED = row.parseStringOption(15).map(removeSpacesStrangeCharsAndToLower),
      STATE = row.parseStringOption(16),
      COUNTRY = row.parseStringOption(17),
      EMAIL_ADDRESS = row.parseStringOption(18),
      PHONE_NUMBER = row.parseStringOption(19),
      MOBILE_PHONE_NUMBER = row.parseStringOption(20),
      FAX_NUMBER = row.parseStringOption(21),
      OPT_OUT = row.parseBooleanOption(22),
      EM_OPT_IN = row.parseBooleanOption(23),
      EM_OPT_OUT = row.parseBooleanOption(24),
      DM_OPT_IN = row.parseBooleanOption(25),
      DM_OPT_OUT = row.parseBooleanOption(26),
      TM_OPT_IN = row.parseBooleanOption(27),
      TM_OPT_OUT = row.parseBooleanOption(28),
      MOB_OPT_IN = row.parseBooleanOption(29),
      MOB_OPT_OUT = row.parseBooleanOption(30),
      FAX_OPT_IN = row.parseBooleanOption(31),
      FAX_OPT_OUT = row.parseBooleanOption(32),
      NR_OF_DISHES = row.parseLongRangeOption(33),
      NR_OF_DISHES_ORIGINAL = row.parseStringOption(33),
      NR_OF_LOCATIONS = row.parseStringOption(34),
      NR_OF_STAFF = row.parseStringOption(35),
      AVG_PRICE = row.parseBigDecimalRangeOption(36),
      AVG_PRICE_ORIGINAL = row.parseStringOption(36),
      DAYS_OPEN = row.parseLongRangeOption(37),
      DAYS_OPEN_ORIGINAL = row.parseStringOption(37),
      WEEKS_CLOSED = row.parseLongRangeOption(38),
      WEEKS_CLOSED_ORIGINAL = row.parseStringOption(38),
      DISTRIBUTOR_NAME = row.parseStringOption(39),
      DISTRIBUTOR_CUSTOMER_NR = row.parseStringOption(40),
      OTM = row.parseStringOption(41),
      OTM_REASON = row.parseStringOption(42),
      OTM_DNR = row.parseBooleanOption(43),
      OTM_DNR_ORIGINAL = row.parseStringOption(43),
      NPS_POTENTIAL = row.parseBigDecimalRangeOption(44),
      NPS_POTENTIAL_ORIGINAL = row.parseStringOption(44),
      SALES_REP = row.parseStringOption(45),
      CONVENIENCE_LEVEL = row.parseStringOption(46),
      PRIVATE_HOUSEHOLD = row.parseBooleanOption(47),
      PRIVATE_HOUSEHOLD_ORIGINAL = row.parseStringOption(47),
      VAT_NUMBER = row.parseStringOption(48),
      OPEN_ON_MONDAY = row.parseBooleanOption(49),
      OPEN_ON_MONDAY_ORIGINAL = row.parseStringOption(49),
      OPEN_ON_TUESDAY = row.parseBooleanOption(50),
      OPEN_ON_TUESDAY_ORIGINAL = row.parseStringOption(50),
      OPEN_ON_WEDNESDAY = row.parseBooleanOption(51),
      OPEN_ON_WEDNESDAY_ORIGINAL = row.parseStringOption(51),
      OPEN_ON_THURSDAY = row.parseBooleanOption(52),
      OPEN_ON_THURSDAY_ORIGINAL = row.parseStringOption(52),
      OPEN_ON_FRIDAY = row.parseBooleanOption(53),
      OPEN_ON_FRIDAY_ORIGINAL = row.parseStringOption(53),
      OPEN_ON_SATURDAY = row.parseBooleanOption(54),
      OPEN_ON_SATURDAY_ORIGINAL = row.parseStringOption(54),
      OPEN_ON_SUNDAY = row.parseBooleanOption(55),
      OPEN_ON_SUNDAY_ORIGINAL = row.parseStringOption(55),
      CHAIN_NAME = row.parseStringOption(56),
      CHAIN_ID = row.parseStringOption(57),
      KITCHEN_TYPE = row.parseStringOption(58)
    )
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
        operatorRecords("COUNTRY_CODE") === countryRecords("COUNTRY_CODE"),
        JoinType.LeftOuter
      )
      .map {
        case (operatorRecord, countryRecord) => Option(countryRecord).fold(operatorRecord) { cr =>
          operatorRecord.copy(
            COUNTRY = Option(cr.COUNTRY),
            COUNTRY_CODE = Option(cr.COUNTRY_CODE)
          )
        }
      }
  }

  override val neededFilePaths = Array("INPUT_FILE", "OUTPUT_FILE")

  override def run(spark: SparkSession, filePaths: Product, storage: Storage): Unit = {
    import spark.implicits._

    val (inputFile: String, outputFile: String) = filePaths

    log.info(s"Generating operator parquet from [$inputFile] to [$outputFile]")

    val operatorRecords = storage
      .readFromCSV(inputFile, separator = csvColumnSeparator)
      .filter(_.length == 59)
      .map(rowToOperatorRecord)

    val countryRecords = storage.countries

    val transformed = transform(spark, operatorRecords, countryRecords)

    storage
      .writeToParquet(transformed, outputFile, partitionBy = "COUNTRY_CODE")
  }
}
