package com.unilever.ohub.spark.tsv2parquet

import java.sql.Timestamp

import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.generic.StringFunctions._
import com.unilever.ohub.spark.sql.JoinType
import com.unilever.ohub.spark.storage.{ CountryRecord, Storage }
import com.unilever.ohub.spark.tsv2parquet.CustomParsers.Implicits._
import org.apache.spark.sql.{ Dataset, Row, SparkSession }

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

  private val csvColumnSeparator = "‰"

  private def rowToContactPersonRecord(row: Row): ContactPersonRecord = {
    val refContactPersonId = row.parseStringOption(0)
    val source = row.parseStringOption(1)
    val countryCode = row.parseStringOption(2)
    val concatId = s"${countryCode.getOrElse("")}~${source.getOrElse("")}~${refContactPersonId.getOrElse("")}"

    val firstName = row.parseStringOption(9).map(removeStrangeCharsToLowerAndTrim)
    val lastName = row.parseStringOption(9).map(removeStrangeCharsToLowerAndTrim)
    val email = row.parseStringOption(25).map(checkEmailValidity)
    
    ContactPersonRecord(
      CONTACT_PERSON_CONCAT_ID = concatId,
      REF_CONTACT_PERSON_ID = refContactPersonId,
      SOURCE = source,
      COUNTRY_CODE = countryCode,
      STATUS = row.parseBooleanOption(3),
      STATUS_ORIGINAL = row.parseStringOption(3),
      REF_OPERATOR_ID = row.parseStringOption(4),
      CP_INTEGRATION_ID = row.parseStringOption(5),
      DATE_CREATED = row.parseDateTimeStampOption(6),
      DATE_MODIFIED = row.parseDateTimeStampOption(7),
      FIRST_NAME = row.parseStringOption(8),
      FIRST_NAME_CLEANSED = firstName,
      LAST_NAME = row.parseStringOption(9),
      LAST_NAME_CLEANSED = lastName,
      BOTH_NAMES_CLEANSED = Some(concatNames(
        firstName.getOrElse(""),
        lastName.getOrElse(""),
        email.getOrElse("")
      )),
      TITLE = row.parseStringOption(10),
      GENDER = row.parseStringOption(11),
      FUNCTION = row.parseStringOption(12),
      LANGUAGE_KEY = row.parseStringOption(13),
      BIRTH_DATE = row.parseDateTimeStampOption(14),
      STREET = row.parseStringOption(15),
      STREET_CLEANSED = row
        .parseStringOption(15)
        .map(removeStrangeCharsToLowerAndTrim)
        .map(_ + row.parseStringOption(16).getOrElse("")),
      HOUSENUMBER = row.parseStringOption(16),
      HOUSENUMBER_EXT = row.parseStringOption(17),
      CITY = row.parseStringOption(18),
      CITY_CLEANSED = row.parseStringOption(18).map(removeSpacesStrangeCharsAndToLower),
      ZIP_CODE = row.parseStringOption(19),
      ZIP_CODE_CLEANSED = row.parseStringOption(19).map(removeSpacesStrangeCharsAndToLower),
      STATE = row.parseStringOption(20),
      COUNTRY = row.parseStringOption(21),
      PREFERRED_CONTACT = row.parseBooleanOption(22),
      PREFERRED_CONTACT_ORIGINAL = row.parseStringOption(22),
      KEY_DECISION_MAKER = row.parseBooleanOption(23),
      KEY_DECISION_MAKER_ORIGINAL = row.parseStringOption(23),
      SCM = row.parseStringOption(24),
      EMAIL_ADDRESS = email,
      EMAIL_ADDRESS_ORIGINAL = row.parseStringOption(25),
      PHONE_NUMBER = row
        .parseStringOption(26)
        .map(phoneNumber => cleanPhoneNumber(phoneNumber, countryCode.getOrElse(""), countryPrefixList)),
      PHONE_NUMBER_ORIGINAL = row.parseStringOption(26),
      MOBILE_PHONE_NUMBER = row
        .parseStringOption(27)
        .map(phoneNumber => cleanPhoneNumber(phoneNumber, countryCode.getOrElse(""), countryPrefixList)),
      MOBILE_PHONE_NUMBER_ORIGINAL = row.parseStringOption(27),
      FAX_NUMBER = row.parseStringOption(28),
      OPT_OUT = row.parseBooleanOption(29),
      OPT_OUT_ORIGINAL = row.parseStringOption(29),
      REGISTRATION_CONFIRMED = row.parseBooleanOption(30),
      REGISTRATION_CONFIRMED_ORIGINAL = row.parseStringOption(30),
      REGISTRATION_CONFIRMED_DATE = row.parseDateTimeStampOption(31),
      REGISTRATION_CONFIRMED_DATE_ORIGINAL = row.parseStringOption(31),
      EM_OPT_IN = row.parseBooleanOption(32),
      EM_OPT_IN_ORIGINAL = row.parseStringOption(32),
      EM_OPT_IN_DATE = row.parseDateTimeStampOption(33),
      EM_OPT_IN_DATE_ORIGINAL = row.parseStringOption(33),
      EM_OPT_IN_CONFIRMED = row.parseBooleanOption(34),
      EM_OPT_IN_CONFIRMED_ORIGINAL = row.parseStringOption(34),
      EM_OPT_IN_CONFIRMED_DATE = row.parseDateTimeStampOption(35),
      EM_OPT_IN_CONFIRMED_DATE_ORIGINAL = row.parseStringOption(35),
      EM_OPT_OUT = row.parseBooleanOption(36),
      EM_OPT_OUT_ORIGINAL = row.parseStringOption(36),
      DM_OPT_IN = row.parseBooleanOption(37),
      DM_OPT_IN_ORIGINAL = row.parseStringOption(37),
      DM_OPT_OUT = row.parseBooleanOption(38),
      DM_OPT_OUT_ORIGINAL = row.parseStringOption(38),
      TM_OPT_IN = row.parseBooleanOption(39),
      TM_OPT_IN_ORIGINAL = row.parseStringOption(39),
      TM_OPT_OUT = row.parseBooleanOption(40),
      TM_OPT_OUT_ORIGINAL = row.parseStringOption(40),
      MOB_OPT_IN = row.parseBooleanOption(41),
      MOB_OPT_IN_ORIGINAL = row.parseStringOption(41),
      MOB_OPT_IN_DATE = row.parseDateTimeStampOption(42),
      MOB_OPT_IN_DATE_ORIGINAL = row.parseStringOption(42),
      MOB_OPT_IN_CONFIRMED = row.parseBooleanOption(43),
      MOB_OPT_IN_CONFIRMED_ORIGINAL = row.parseStringOption(43),
      MOB_OPT_IN_CONFIRMED_DATE = row.parseDateTimeStampOption(44),
      MOB_OPT_IN_CONFIRMED_DATE_ORIGINAL = row.parseStringOption(44),
      MOB_OPT_OUT = row.parseBooleanOption(45),
      MOB_OPT_OUT_ORIGINAL = row.parseStringOption(45),
      FAX_OPT_IN = row.parseBooleanOption(46),
      FAX_OPT_IN_ORIGINAL = row.parseStringOption(46),
      FAX_OPT_OUT = row.parseBooleanOption(47),
      FAX_OPT_OUT_ORIGINAL = row.parseStringOption(47)
    )
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
        contactPersonRecords("COUNTRY_CODE") === countryRecords("COUNTRY_CODE"),
        JoinType.LeftOuter
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

    log.info(s"Generating parquet from [$inputFile] to [$outputFile]")

    val contactPersonRecords = storage
      .readFromCSV(inputFile, separator = csvColumnSeparator)
      .filter(_.length == 48)
      .map(rowToContactPersonRecord)

    val countryRecords = storage.countries

    val transformed = transform(spark, contactPersonRecords, countryRecords)

    storage
      .writeToParquet(transformed, outputFile, partitionBy = "COUNTRY_CODE")
  }
}
