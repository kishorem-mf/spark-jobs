package com.unilever.ohub.spark.tsv2parquet

import java.sql.Timestamp

import com.unilever.ohub.spark.tsv2parquet.CustomParsers._
import org.apache.spark.sql.SaveMode.Overwrite
import org.apache.spark.sql.SparkSession

case class ProductRecord(
                        REF_PRODUCT_ID:Option[String], SOURCE:Option[String], COUNTRY_CODE:Option[String], STATUS:Option[Boolean], STATUS_ORIGINAL:Option[String], DATE_CREATED:Option[Timestamp],
                        DATE_CREATED_ORIGINAL:Option[String], DATE_MODIFIED:Option[Timestamp], DATE_MODIFIED_ORIGINAL:Option[String], PRODUCT_NAME:Option[String], EAN_CU:Option[String], EAN_DU:Option[String],
                        MRDR:Option[String], UNIT:Option[String], UNIT_PRICE:Option[BigDecimal], UNIT_PRICE_ORIGINAL:Option[String], UNIT_PRICE_CURRENCY:Option[String]
                        )

object ProductConverter extends App {
  if (args.length != 2) {
    println("specify INPUT_FILE OUTPUT_FILE")
    sys.exit(1)
  }

  val inputFile = args(0)
  val outputFile = args(1)

  println(s"Generating orders parquet from [$inputFile] to [$outputFile]")

  val spark = SparkSession
    .builder()
    .appName(this.getClass.getSimpleName)
    .getOrCreate()

  import spark.implicits._

  lazy val expectedPartCount = 13

  val startOfJob = System.currentTimeMillis()
  val lines = spark.read.textFile(inputFile)

  val records = lines
    .filter(line => !line.isEmpty && !line.startsWith("REF_PRODUCT_ID"))
    .map(line => line.split("‰", -1))
    .map(lineParts => {
      checkLineLength(lineParts, expectedPartCount)
      try {
        ProductRecord(
          REF_PRODUCT_ID = parseStringOption(lineParts(0)),
          SOURCE = parseStringOption(lineParts(1)),
          COUNTRY_CODE = parseStringOption(lineParts(2)),
          STATUS = parseBoolOption(lineParts(3)),
          STATUS_ORIGINAL = parseStringOption(lineParts(3)),
          DATE_CREATED = parseDateTimeStampOption(lineParts(4)),
          DATE_CREATED_ORIGINAL = parseStringOption(lineParts(4)),
          DATE_MODIFIED = parseDateTimeStampOption(lineParts(5)),
          DATE_MODIFIED_ORIGINAL = parseStringOption(lineParts(5)),
          PRODUCT_NAME = parseStringOption(lineParts(6)),
          EAN_CU = parseStringOption(lineParts(7)),
          EAN_DU = parseStringOption(lineParts(8)),
          MRDR = parseStringOption(lineParts(9)),
          UNIT = parseStringOption(lineParts(10)),
          UNIT_PRICE = parseBigDecimalOption(lineParts(11)),
          UNIT_PRICE_ORIGINAL = parseStringOption(lineParts(11)),
          UNIT_PRICE_CURRENCY = parseStringOption(lineParts(12))
        )
      } catch {
        case e:Exception => throw new RuntimeException(s"Exception while parsing line: ${lineParts.mkString("‰")}", e)
      }
    })

  records.write.mode(Overwrite).format("parquet").save(outputFile)

  records.printSchema()

  val count = records.count()
  println(s"Processed $count records in ${(System.currentTimeMillis - startOfJob) / 1000}s")
  println("Done")
}
