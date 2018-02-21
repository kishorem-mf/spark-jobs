package com.unilever.ohub.spark.tsv2parquet

import java.util.InputMismatchException

import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.generic.StringFunctions.{ removeSpacesStrangeCharsAndToLower, removeStrangeCharsToLowerAndTrim }
import com.unilever.ohub.spark.sql.JoinType
import com.unilever.ohub.spark.storage.Storage
import CustomParsers.Implicits._
import com.unilever.ohub.spark.data.{ CountryRecord, OperatorRecord }
import com.unilever.ohub.spark.generic.StringFunctions
import org.apache.spark.sql.{ Dataset, Row, SparkSession }

object OperatorConverter extends SparkJob  {
  private val csvColumnSeparator = "‰"

  private def rowToOperatorRecord(row: Row): OperatorRecord = {
    val refOperatorId = row.parseStringOption(0)
    val source = row.parseStringOption(1)
    val countryCode = row.parseStringOption(2)
    val concatId = StringFunctions.createConcatId(countryCode, source, refOperatorId)

    OperatorRecord(
      operatorConcatId = concatId,
      refOperatorId = refOperatorId,
      source = source,
      countryCode = countryCode,
      status = row.parseBooleanOption(3),
      statusOriginal = row.parseStringOption(3),
      name = row.parseStringOption(4),
      nameCleansed = row.parseStringOption(4).map(removeStrangeCharsToLowerAndTrim),
      operatorIntegrationId = row.parseStringOption(5),
      dateCreated = row.parseDateTimeStampOption(6),
      dateModified = row.parseDateTimeStampOption(7),
      channel = row.parseStringOption(8),
      subChannel = row.parseStringOption(9),
      region = row.parseStringOption(10),
      street = row.parseStringOption(11),
      streetCleansed = row
        .parseStringOption(11)
        .map(removeStrangeCharsToLowerAndTrim)
        .map(_ + row.parseStringOption(12).getOrElse("")),
      houseNumber = row.parseStringOption(12),
      houseNumberExt = row.parseStringOption(123),
      city = row.parseStringOption(14),
      cityCleansed = row.parseStringOption(14).map(removeSpacesStrangeCharsAndToLower),
      zipCode = row.parseStringOption(15),
      zipCodeCleansed = row.parseStringOption(15).map(removeSpacesStrangeCharsAndToLower),
      state = row.parseStringOption(16),
      country = row.parseStringOption(17),
      emailAddress = row.parseStringOption(18),
      phoneNumber = row.parseStringOption(19),
      mobilePhoneNumber = row.parseStringOption(20),
      faxNumber = row.parseStringOption(21),
      optOut = row.parseBooleanOption(22),
      emailOptIn = row.parseBooleanOption(23),
      emailOptOut = row.parseBooleanOption(24),
      directMailOptIn = row.parseBooleanOption(25),
      directMailOptOut = row.parseBooleanOption(26),
      telemarketingOptIn = row.parseBooleanOption(27),
      telemarketingOptOut = row.parseBooleanOption(28),
      mobileOptIn = row.parseBooleanOption(29),
      mobileOptOut = row.parseBooleanOption(30),
      faxOptIn = row.parseBooleanOption(31),
      faxOptOut = row.parseBooleanOption(32),
      nrOfDishes = row.parseLongRangeOption(33),
      nrOfDishesOriginal = row.parseStringOption(33),
      nrOfLocations = row.parseStringOption(34),
      nrOfStaff = row.parseStringOption(35),
      avgPrice = row.parseBigDecimalRangeOption(36),
      avgPriceOriginal = row.parseStringOption(36),
      daysOpen = row.parseLongRangeOption(37),
      daysOpenOriginal = row.parseStringOption(37),
      weeksClosed = row.parseLongRangeOption(38),
      weeksClosedOriginal = row.parseStringOption(38),
      distributorName = row.parseStringOption(39),
      distributorCustomerNr = row.parseStringOption(40),
      otm = row.parseStringOption(41),
      otmReason = row.parseStringOption(42),
      otmDnr = row.parseBooleanOption(43),
      otmDnrOriginal = row.parseStringOption(43),
      npsPotential = row.parseBigDecimalRangeOption(44),
      npsPotentialOriginal = row.parseStringOption(44),
      salesRep = row.parseStringOption(45),
      convenienceLevel = row.parseStringOption(46),
      privateHousehold = row.parseBooleanOption(47),
      privateHouseholdOriginal = row.parseStringOption(47),
      vatNumber = row.parseStringOption(48),
      openOnMonday = row.parseBooleanOption(49),
      openOnMondayOriginal = row.parseStringOption(49),
      openOnTuesday = row.parseBooleanOption(50),
      openOnTuesdayOriginal = row.parseStringOption(50),
      openOnWednesday = row.parseBooleanOption(51),
      openOnWednesdayOriginal = row.parseStringOption(51),
      openOnThursday = row.parseBooleanOption(52),
      openOnThursdayOriginal = row.parseStringOption(52),
      openOnFriday = row.parseBooleanOption(53),
      openOnFridayOriginal = row.parseStringOption(53),
      openOnSaturday = row.parseBooleanOption(54),
      openOnSaturdayOriginal = row.parseStringOption(54),
      openOnSunday = row.parseBooleanOption(55),
      openOnSundayOriginal = row.parseStringOption(55),
      chainName = row.parseStringOption(56),
      chainId = row.parseStringOption(57),
      kitchenType = row.parseStringOption(58)
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
        operatorRecords("countryCode") === countryRecords("countryCode"),
        JoinType.LeftOuter
      )
      .map {
        case (operatorRecord, countryRecord) => Option(countryRecord).fold(operatorRecord) { cr =>
          operatorRecord.copy(
            country = Option(cr.countryName),
            countryCode = Option(cr.countryCode)
          )
        }
      }
  }

  override val neededFilePaths = Array("INPUT_FILE", "OUTPUT_FILE")

  override def run(spark: SparkSession, filePaths: Product, storage: Storage): Unit = {
    import spark.implicits._

    val (inputFile: String, outputFile: String) = filePaths

    log.info(s"Generating operator parquet from [$inputFile] to [$outputFile]")

    val requiredNrOfColumns = 59
    val operatorRecords = storage
      .readFromCSV(inputFile, separator = csvColumnSeparator)
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
      .map(rowToOperatorRecord)

    val countryRecords = storage.createCountries

    val transformed = transform(spark, operatorRecords, countryRecords)

    storage
      .writeToParquet(transformed, outputFile, partitionBy = "countryCode")
  }
}
