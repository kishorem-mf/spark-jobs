package com.unilever.ohub.spark.tsv2parquet

import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.sql.JoinType
import CustomParsers.Implicits._
import com.unilever.ohub.spark.data.{ CountryRecord, EmContactPersonRecord }
import org.apache.spark.sql.{ Dataset, Row, SparkSession }

object EmContactPersonConverter extends SparkJob {
  private val csvColumnSeparator = "‰"

  private def rowToEmContactPersonRecord(row: Row): EmContactPersonRecord = {
    EmContactPersonRecord(
      contactPersonId = row.parseLongRangeOption(0),
      countryCode = row.parseStringOption(1),
      emSourceId = row.parseStringOption(2),
      webServiceRequestId = row.parseLongRangeOption(3),
      webupdaterId = row.parseLongRangeOption(4),
      emailAddress = row.parseStringOption(5),
      mobilePhone = row.parseLongRangeOption(6),
      phone = row.parseLongRangeOption(7),
      fax = row.parseLongRangeOption(8),
      title = row.parseStringOption(9), // enum: Mr., ...
      firstName = row.parseStringOption(10),
      lastName = row.parseStringOption(11),
      optIn = row.parseBooleanOption(12), // 0/1
      jobTitle = row.parseStringOption(13), // enum
      language = row.parseStringOption(14), // enum
      operatorName = row.parseStringOption(15),
      typeOfBusiness = row.parseStringOption(16), // enum
      street = row.parseStringOption(17),
      houseNumber = row.parseStringOption(18),
      houseNumberExt = row.parseStringOption(19), // empty
      postcode = row.parseStringOption(20),
      city = row.parseStringOption(21),
      state = row.parseStringOption(22), // empty
      country = row.parseStringOption(23), // enum
      typeOfCuisine = row.parseStringOption(24), // enum
      nrOfCoversPerDay = row.parseLongRangeOption(25),
      nrOfLocations = row.parseLongRangeOption(26),
      nrOfKitchenStaff = row.parseLongRangeOption(27),
      primaryDistributor = row.parseStringOption(28),
      gender = row.parseStringOption(29), // enum: M/F
      operatorRefId = row.parseLongRangeOption(30), // empty
      optInDate = row.parseDateTimeStampOption(31),
      confirmedOptIn = row.parseBooleanOption(32), // 0/1
      confirmedOptInDate = row.parseDateTimeStampOption(33),
      distributorCustomerId = row.parseStringOption(34),
      privateHousehold = row.parseBooleanOption(35), // 0/1
      vat = row.parseStringOption(36), // ?, empty
      openOnMonday = row.parseBooleanOption(37), // 0/1
      openOnTuesday = row.parseBooleanOption(38), // 0/1
      openOnWednesday = row.parseBooleanOption(39), // 0/1
      openOnThursday = row.parseBooleanOption(40), // 0/1
      openOnFriday = row.parseBooleanOption(41), // 0/1
      openOnSaturday = row.parseBooleanOption(42), // 0/1
      openOnSunday = row.parseBooleanOption(43) // 0/1
    )
  }

  def transform(
    spark: SparkSession,
    records: Dataset[EmContactPersonRecord],
    countryRecords: Dataset[CountryRecord]
  ): Dataset[EmContactPersonRecord] = {
    import spark.implicits._

    records
      .joinWith(
        countryRecords,
        records("countryCode") === countryRecords("countryCode"),
        JoinType.LeftOuter
      )
      .map {
        case (record, countryRecord) => Option(countryRecord).fold(record) { cr =>
          record.copy(
            countryCode = Option(cr.countryCode)
          )
        }
      }
  }

  override val neededFilePaths = Array("INPUT_FILE", "OUTPUT_FILE")

  override def run(spark: SparkSession, filePaths: Product, storage: Storage): Unit = {
    import spark.implicits._

    val (inputFile: String, outputFile: String) = filePaths

    log.info(s"Generating orders parquet from [$inputFile] to [$outputFile]")

    val records = storage
      .readFromCSV(inputFile, separator = csvColumnSeparator)
      .filter(_.length == 44)
      .map(rowToEmContactPersonRecord)

    val countryRecords = storage.countries

    val transformed = transform(spark, records, countryRecords)

    storage
      .writeToParquet(transformed, outputFile, partitionBy = "countryCode")
  }
}
