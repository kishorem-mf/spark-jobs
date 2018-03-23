package com.unilever.ohub.spark.tsv2parquet

import java.util.InputMismatchException

import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.generic.StringFunctions._
import com.unilever.ohub.spark.sql.JoinType
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.tsv2parquet.CustomParsers.Implicits._
import com.unilever.ohub.spark.data.{ ContactPersonRecord, CountryRecord }
import com.unilever.ohub.spark.generic.StringFunctions
import org.apache.spark.sql.{ Dataset, Row, SparkSession }

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
    val concatId = StringFunctions.createConcatId(countryCode, source, refContactPersonId)

    val firstName = row.parseStringOption(9).map(removeStrangeCharsToLowerAndTrim)
    val lastName = row.parseStringOption(9).map(removeStrangeCharsToLowerAndTrim)
    val email = row.parseStringOption(25).map(checkEmailValidity)

    ContactPersonRecord(
      contactPersonConcatId = concatId,
      refContactPersonId = refContactPersonId,
      source = source,
      countryCode = countryCode,
      status = row.parseBooleanOption(3),
      statusOriginal = row.parseStringOption(3),
      refOperatorId = row.parseStringOption(4),
      contactPersonIntegrationId = row.parseStringOption(5),
      dateCreated = row.parseDateTimeStampOption(6),
      dateModified = row.parseDateTimeStampOption(7),
      firstName = row.parseStringOption(8),
      firstNameCleansed = firstName,
      lastName = row.parseStringOption(9),
      lastNameCleansed = lastName,
      bothNamesCleansed = Some(concatNames(
        firstName.getOrElse(""),
        lastName.getOrElse(""),
        email.getOrElse("")
      )),
      title = row.parseStringOption(10),
      gender = row.parseStringOption(11),
      function = row.parseStringOption(12),
      languageKey = row.parseStringOption(13),
      birthDate = row.parseDateTimeStampOption(14),
      street = row.parseStringOption(15),
      streetCleansed = row
        .parseStringOption(15)
        .map(removeStrangeCharsToLowerAndTrim)
        .map(_ + row.parseStringOption(16).getOrElse("")),
      houseNumber = row.parseStringOption(16),
      houseNumberExt = row.parseStringOption(17),
      city = row.parseStringOption(18),
      cityCleansed = row.parseStringOption(18).map(removeSpacesStrangeCharsAndToLower),
      zipCode = row.parseStringOption(19),
      zipCodeCleansed = row.parseStringOption(19).map(removeSpacesStrangeCharsAndToLower),
      state = row.parseStringOption(20),
      country = row.parseStringOption(21),
      preferredContact = row.parseBooleanOption(22),
      preferredContactOriginal = row.parseStringOption(22),
      keyDecisionMaker = row.parseBooleanOption(23),
      keyDecisionMakerOriginal = row.parseStringOption(23),
      scm = row.parseStringOption(24),
      emailAddress = email,
      emailAddressOriginal = row.parseStringOption(25),
      phoneNumber = row
        .parseStringOption(26)
        .map(phoneNumber => cleanPhoneNumber(phoneNumber, countryCode.getOrElse(""), countryPrefixList)),
      phoneNumberOriginal = row.parseStringOption(26),
      mobilePhoneNumber = row
        .parseStringOption(27)
        .map(phoneNumber => cleanPhoneNumber(phoneNumber, countryCode.getOrElse(""), countryPrefixList)),
      mobilePhoneNumberOriginal = row.parseStringOption(27),
      faxNumber = row.parseStringOption(28),
      optOut = row.parseBooleanOption(29),
      optOutOriginal = row.parseStringOption(29),
      registrationConfirmed = row.parseBooleanOption(30),
      registrationConfirmedOriginal = row.parseStringOption(30),
      registrationConfirmedDate = row.parseDateTimeStampOption(31),
      registrationConfirmedDateOriginal = row.parseStringOption(31),
      emailOptIn = row.parseBooleanOption(32),
      emailOptInOriginal = row.parseStringOption(32),
      emailOptInDate = row.parseDateTimeStampOption(33),
      emailOptInDateOriginal = row.parseStringOption(33),
      emailOptInConfirmed = row.parseBooleanOption(34),
      emailOptInConfirmedOriginal = row.parseStringOption(34),
      emailOptInConfirmedDate = row.parseDateTimeStampOption(35),
      emailOptInConfirmedDateOriginal = row.parseStringOption(35),
      emailOptOut = row.parseBooleanOption(36),
      emailOptOutOriginal = row.parseStringOption(36),
      directMailOptIn = row.parseBooleanOption(37),
      directMailOptInOriginal = row.parseStringOption(37),
      directMailOptOut = row.parseBooleanOption(38),
      directMailOptOutOriginal = row.parseStringOption(38),
      telemarketingOptIn = row.parseBooleanOption(39),
      telemarketingOptInOriginal = row.parseStringOption(39),
      telemarketingOptOut = row.parseBooleanOption(40),
      telemarketingOptOutOriginal = row.parseStringOption(40),
      mobileOptIn = row.parseBooleanOption(41),
      mobileOptInOriginal = row.parseStringOption(41),
      mobileOptInDate = row.parseDateTimeStampOption(42),
      mobileOptInDateOriginal = row.parseStringOption(42),
      mobileOptInConfirmed = row.parseBooleanOption(43),
      mobileOptInConfirmedOriginal = row.parseStringOption(43),
      mobileOptInConfirmedDate = row.parseDateTimeStampOption(44),
      mobileOptInConfirmedDateOriginal = row.parseStringOption(44),
      mobileOptOut = row.parseBooleanOption(45),
      mobileOptOutOriginal = row.parseStringOption(45),
      faxOptIn = row.parseBooleanOption(46),
      faxOptInOriginal = row.parseStringOption(46),
      faxOptOut = row.parseBooleanOption(47),
      faxOptOutOriginal = row.parseStringOption(47)
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
        contactPersonRecords("countryCode") === countryRecords("countryCode"),
        JoinType.LeftOuter
      )
      .map {
        case (contactPersonRecord, countryRecord) => Option(countryRecord).fold(contactPersonRecord) { cr =>
          contactPersonRecord.copy(
            country = Option(cr.countryName),
            countryCode = Option(cr.countryCode)
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

    val requiredNrOfColumns = 48
    val contactPersonRecords = storage
      .readFromCsv(inputFile, fieldSeparator = csvColumnSeparator)
      .filter { row =>
        if (row.length != requiredNrOfColumns) {
          throw new InputMismatchException(
            s"An input CSV row did not have the required $requiredNrOfColumns columns\n${row.toString()}"
          )
          false
        } else {
          true
        }
      }
      .map(rowToContactPersonRecord)

    val countryRecords = storage.createCountries

    val transformed = transform(spark, contactPersonRecords, countryRecords)

    storage
      .writeToParquet(transformed, outputFile, partitionBy = Seq("countryCode"))
  }
}
