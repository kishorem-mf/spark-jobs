package com.unilever.ohub.spark.tsv2parquet

import java.sql.Timestamp

import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.generic.StringFunctions._
import com.unilever.ohub.spark.sql.LeftOuter
import com.unilever.ohub.spark.storage.{ CountryRecord, Storage }
import com.unilever.ohub.spark.tsv2parquet.CustomParsers._
import org.apache.spark.sql.{ Dataset, SparkSession }

case class ContactPersonRecord(
  CONTACT_PERSON_CONCAT_ID: String, REF_CONTACT_PERSON_ID: Option[String], SOURCE: Option[String],
  COUNTRY_CODE: Option[String], STATUS: Option[Boolean], STATUS_ORIGINAL: Option[String],
  REF_OPERATOR_ID: Option[String], CP_INTEGRATION_ID: Option[String],
  DATE_CREATED: Option[Timestamp], DATE_MODIFIED: Option[Timestamp], FIRST_NAME: Option[String],
  FIRST_NAME_CLEANSED: Option[String], LAST_NAME: Option[String], LAST_NAME_CLEANSED: Option[String],
  BOTH_NAMES_CLEANSED: Option[String], TITLE: Option[String],
  GENDER: Option[String], FUNCTION: Option[String], LANGUAGE_KEY: Option[String],
  BIRTH_DATE: Option[Timestamp], STREET: Option[String], STREET_CLEANSED: Option[String],
  HOUSENUMBER: Option[String],
  HOUSENUMBER_EXT: Option[String], CITY: Option[String], CITY_CLEANSED: Option[String],
  ZIP_CODE: Option[String], ZIP_CODE_CLEANSED: Option[String], STATE: Option[String], COUNTRY: Option[String],
  PREFERRED_CONTACT: Option[Boolean], PREFERRED_CONTACT_ORIGINAL: Option[String],
  KEY_DECISION_MAKER: Option[Boolean], KEY_DECISION_MAKER_ORIGINAL: Option[String], SCM: Option[String],
  EMAIL_ADDRESS: Option[String], EMAIL_ADDRESS_ORIGINAL: Option[String],
  PHONE_NUMBER: Option[String], PHONE_NUMBER_ORIGINAL: Option[String], MOBILE_PHONE_NUMBER: Option[String],
  MOBILE_PHONE_NUMBER_ORIGINAL: Option[String], FAX_NUMBER: Option[String], OPT_OUT: Option[Boolean],
  OPT_OUT_ORIGINAL: Option[String],
  REGISTRATION_CONFIRMED: Option[Boolean], REGISTRATION_CONFIRMED_ORIGINAL: Option[String],
  REGISTRATION_CONFIRMED_DATE: Option[Timestamp], REGISTRATION_CONFIRMED_DATE_ORIGINAL: Option[String],
  EM_OPT_IN: Option[Boolean], EM_OPT_IN_ORIGINAL: Option[String], EM_OPT_IN_DATE: Option[Timestamp],
  EM_OPT_IN_DATE_ORIGINAL: Option[String], EM_OPT_IN_CONFIRMED: Option[Boolean],
  EM_OPT_IN_CONFIRMED_ORIGINAL: Option[String], EM_OPT_IN_CONFIRMED_DATE: Option[Timestamp],
  EM_OPT_IN_CONFIRMED_DATE_ORIGINAL: Option[String],
  EM_OPT_OUT: Option[Boolean], EM_OPT_OUT_ORIGINAL: Option[String], DM_OPT_IN: Option[Boolean],
  DM_OPT_IN_ORIGINAL: Option[String], DM_OPT_OUT: Option[Boolean], DM_OPT_OUT_ORIGINAL: Option[String],
  TM_OPT_IN: Option[Boolean], TM_OPT_IN_ORIGINAL: Option[String], TM_OPT_OUT: Option[Boolean],
  TM_OPT_OUT_ORIGINAL: Option[String], MOB_OPT_IN: Option[Boolean], MOB_OPT_IN_ORIGINAL: Option[String],
  MOB_OPT_IN_DATE: Option[Timestamp], MOB_OPT_IN_DATE_ORIGINAL: Option[String],
  MOB_OPT_IN_CONFIRMED: Option[Boolean], MOB_OPT_IN_CONFIRMED_ORIGINAL: Option[String],
  MOB_OPT_IN_CONFIRMED_DATE: Option[Timestamp], MOB_OPT_IN_CONFIRMED_DATE_ORIGINAL: Option[String],
  MOB_OPT_OUT: Option[Boolean], MOB_OPT_OUT_ORIGINAL: Option[String], FAX_OPT_IN: Option[Boolean],
  FAX_OPT_IN_ORIGINAL: Option[String], FAX_OPT_OUT: Option[Boolean], FAX_OPT_OUT_ORIGINAL: Option[String]
)

object ContactPersonConverter extends SparkJob {
  private val countryList = Array("CU", "CX", "FI", "GS", "GY", "KE", "KY", "LV", "LY", "MM", "MP", "MS", "NC",
    "NO", "NZ", "AO", "AS", "AW", "BH", "BN", "PY", "RU", "SO", "SZ", "TC", "TN", "VA", "VE", "WF", "CR",
    "DJ", "ES", "FM", "GH", "GT", "GU", "IL", "IO", "LI", "MH", "MR", "NA", "NG", "NP", "AI", "AR", "BA",
    "BI", "BZ", "PM", "PT", "PW", "TD", "TR", "TZ", "CH", "CI", "CK", "CN", "CY", "CZ", "EC", "GM", "IE",
    "IS", "IT", "JP", "KR", "LK", "LR", "MG", "MQ", "NE", "PG", "AT", "BD", "BF", "BG", "BO", "CA", "SA",
    "SG", "ST", "SX", "TH", "TM", "VG", "VU", "CL", "CO", "DO", "EE", "FJ", "FK", "FR", "GD", "GE", "GF",
    "GG", "HT", "ID", "IM", "JE", "JM", "JO", "KG", "KP", "LB", "LC", "MN", "MT", "NR", "AG", "AL", "AM",
    "AX", "AZ", "PL", "QA", "SB", "SS", "TK", "TT", "UG", "WS", "YT", "ZA", "EH", "GR", "HN", "IN", "KH",
    "KW", "LU", "MV", "MX", "MZ", "PA", "AD", "AE", "PR", "SE", "TV", "UZ", "VC", "VI", "CW", "DE", "GB",
    "GI", "GL", "GP", "GW", "HR", "HU", "IQ", "KI", "KM", "MA", "MC", "ME", "ML", "NF", "PF", "AU", "BE",
    "BL", "BT", "PK", "PS", "RE", "RO", "RS", "SD", "SH", "SI", "SL", "SM", "TG", "TW", "UA", "YE", "ZW",
    "CM", "DM", "EG", "ET", "GN", "KZ", "LA", "LS", "LT", "MU", "MY", "NL", "OM", "PE", "PH", "AF", "BB",
    "BJ", "BM", "BS", "CC", "RW", "SK", "TO", "US", "VN", "CG", "CV", "DK", "DZ", "ER", "FO", "GA", "GQ",
    "HK", "IR", "KN", "MD", "MK", "MO", "MW", "NI", "NU", "BQ", "BR", "BW", "BY", "CF", "SC", "SN", "SR",
    "SV", "SY", "TJ", "TL", "UY", "ZM")
  private val prefixList = Array("53", "61", "358", "500", "592", "254", "1", "371", "218", "95", "1", "1", "687",
    "47", "64", "244", "1", "297", "973", "673", "595", "7", "252", "268", "1", "216", "39", "58", "681",
    "506", "253", "34", "691", "233", "502", "1", "972", "246", "423", "692", "222", "264", "234", "977",
    "1", "54", "387", "257", "501", "508", "351", "680", "235", "90", "255", "41", "225", "682", "86",
    "357", "420", "593", "220", "353", "354", "39", "81", "82", "94", "231", "261", "596", "227", "675",
    "43", "880", "226", "359", "591", "1", "966", "65", "239", "1", "66", "993", "1", "678", "56", "57",
    "1", "372", "679", "500", "33", "1", "995", "594", "44", "509", "62", "44", "44", "1", "962", "996",
    "850", "961", "1", "976", "356", "674", "1", "355", "374", "358", "994", "48", "974", "677", "211",
    "690", "1", "256", "685", "262", "27", "212", "30", "504", "91", "855", "965", "352", "960", "52",
    "258", "507", "376", "971", "1", "46", "688", "998", "1", "1", "599", "49", "44", "350", "299", "590",
    "245", "385", "36", "964", "686", "269", "212", "377", "382", "223", "672", "689", "61", "32", "590",
    "975", "92", "97", "262", "40", "381", "249", "290", "386", "232", "378", "228", "886", "380", "967",
    "263", "237", "1", "20", "251", "224", "7", "856", "266", "370", "230", "60", "31", "968", "51", "63",
    "93", "1", "229", "1", "1", "61", "250", "421", "676", "1", "84", "243", "238", "45", "213", "291",
    "298", "241", "240", "852", "98", "1", "373", "389", "853", "265", "505", "683", "599", "55", "267",
    "375", "236", "248", "221", "597", "503", "963", "992", "670", "598", "260")
  private val countryPrefixList = countryList zip prefixList.toList

  private def linePartsToContactPersonRecord(lineParts: Array[String]): ContactPersonRecord = {
    try {
      ContactPersonRecord(
        CONTACT_PERSON_CONCAT_ID = s"${lineParts(2)}~${lineParts(1)}~${lineParts(0)}",
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
        BOTH_NAMES_CLEANSED = parseStringOption(
          concatNames(
            removeStrangeCharsToLowerAndTrim(lineParts(8)),
            removeStrangeCharsToLowerAndTrim(lineParts(9)),
            checkEmailValidity(lineParts(25))
          )
        ),
        TITLE = parseStringOption(lineParts(10)),
        GENDER = parseStringOption(lineParts(11)),
        FUNCTION = parseStringOption(lineParts(12)),
        LANGUAGE_KEY = parseStringOption(lineParts(13)),
        BIRTH_DATE = parseDateTimeStampOption(lineParts(14)),
        STREET = parseStringOption(lineParts(15)),
        STREET_CLEANSED = parseStringOption(removeStrangeCharsToLowerAndTrim(lineParts(15).concat
        (lineParts(16)))),
        HOUSENUMBER = parseStringOption(lineParts(16)),
        HOUSENUMBER_EXT = parseStringOption(lineParts(17)),
        CITY = parseStringOption(lineParts(18)),
        CITY_CLEANSED = parseStringOption(removeSpacesStrangeCharsAndToLower(lineParts(18))),
        ZIP_CODE = parseStringOption(lineParts(19)),
        ZIP_CODE_CLEANSED = parseStringOption(removeSpacesStrangeCharsAndToLower(lineParts(19))),
        STATE = parseStringOption(lineParts(20)),
        COUNTRY = parseStringOption(lineParts(21)),
        PREFERRED_CONTACT = parseBoolOption(lineParts(22)),
        PREFERRED_CONTACT_ORIGINAL = parseStringOption(lineParts(22)),
        KEY_DECISION_MAKER = parseBoolOption(lineParts(23)),
        KEY_DECISION_MAKER_ORIGINAL = parseStringOption(lineParts(23)),
        SCM = parseStringOption(lineParts(24)),
        EMAIL_ADDRESS = parseStringOption(checkEmailValidity(lineParts(25))),
        EMAIL_ADDRESS_ORIGINAL = parseStringOption(lineParts(25)),
        PHONE_NUMBER = parseStringOption(cleanPhoneNumber(lineParts(26), lineParts(2), countryPrefixList)),
        PHONE_NUMBER_ORIGINAL = parseStringOption(lineParts(26)),
        MOBILE_PHONE_NUMBER = parseStringOption(
          cleanPhoneNumber(lineParts(27), lineParts(2), countryPrefixList)
        ),
        MOBILE_PHONE_NUMBER_ORIGINAL = parseStringOption(lineParts(27)),
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
      case e: Exception =>
        throw new RuntimeException(s"Exception while parsing line: ${lineParts.mkString("‰")}", e)
    }
  }

  def transform(
    spark: SparkSession,
    contactPersonRecords: Dataset[ContactPersonRecord],
    countryRecords: Dataset[CountryRecord]
  ): Dataset[ContactPersonRecord] = {
    import spark.implicits._

    contactPersonRecords
      .filter(_ != null)
      .joinWith(
        countryRecords,
        countryRecords("COUNTRY").isNotNull and
          contactPersonRecords("COUNTRY_CODE") === countryRecords("COUNTRY_CODE"),
        LeftOuter
      )
      .map {
        case (contactPersonRecord, countryRecord) => Option(countryRecord).fold(contactPersonRecord) { cr =>
          contactPersonRecord.copy(
            COUNTRY = Option(cr.COUNTRY),
            COUNTRY_CODE = Option(cr.COUNTRY_CODE)
          )
        }
      }
  }

  override val neededFilePaths = Array("INPUT_FILE", "OUTPUT_FILE")

  override def run(
    spark: SparkSession,
    filePaths: Product,
    storage: Storage
  ): Unit = {
    import spark.implicits._

    val (inputFile: String, outputFile: String) = filePaths

    val hasValidLineLength = CustomParsers.hasValidLineLength(48) _

    val contactPersonRecords = storage
      .readFromCSV[String](inputFile)
      .map(_.split("‰", -1))
      .filter(hasValidLineLength)
      .map(linePartsToContactPersonRecord)

    val countryRecords = storage.countries

    val transformed = transform(spark, contactPersonRecords, countryRecords)

    storage.writeToParquet(transformed, outputFile)
  }
}
