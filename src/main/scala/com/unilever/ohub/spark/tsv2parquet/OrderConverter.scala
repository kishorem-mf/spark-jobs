package com.unilever.ohub.spark.tsv2parquet

import java.sql.Timestamp

import com.unilever.ohub.spark.tsv2parquet.CustomParsers._
import org.apache.spark.sql.SaveMode.Overwrite
import org.apache.spark.sql.SparkSession


case class OrderRecord(REF_ORDER_ID:String, SOURCE:String, COUNTRY_CODE:String, STATUS:Option[Boolean], STATUS_ORIGINAL:String, REF_OPERATOR_ID:String,
                       REF_CONTACT_PERSON_ID:String, ORDER_TYPE:String, TRANSACTION_DATE:Option[Timestamp], TRANSACTION_DATE_ORIGINAL:String, REF_PRODUCT_ID:String, QUANTITY:Option[Long], QUANTITY_ORIGINAL:String,
                       ORDER_LINE_VALUE:Option[BigDecimal], ORDER_LINE_VALUE_ORIGINAL:String, ORDER_VALUE:Option[BigDecimal], ORDER_VALUE_ORIGINAL:String, WHOLESALER:String, CAMPAIGN_CODE:String, CAMPAIGN_NAME:String,
                       UNIT_PRICE:Option[BigDecimal], UNIT_PRICE_ORIGINAL:String, CURRENCY_CODE:String, DATE_CREATED:Option[Timestamp], DATE_MODIFIED:Option[Timestamp])

object OrderConverter extends App {
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

  val startOfJob = System.currentTimeMillis()
  val lines = spark.read.textFile(inputFile)

  def checkOrderType(orderType: String): String = {
    if (Seq("", "SSD", "TRANSFER", "DIRECT", "UNKNOWN", "MERCHANDISE", "SAMPLE", "EVENT", "WEB", "BIN", "OTHER").contains(orderType)) {
      orderType
    } else {
      throw new IllegalArgumentException("Unknown ORDER_TYPE: " + orderType)
    }
  }

  val records = lines
    .filter(line => !line.isEmpty && !line.startsWith("REF_ORDER_ID"))
    .map(line => line.split("‰", -1))
    .map(lineParts => {
      checkLineLength(lineParts, 19)
      try {
        OrderRecord(
          REF_ORDER_ID = lineParts(0),
          SOURCE = lineParts(1),
          COUNTRY_CODE = lineParts(2),
          STATUS = parseBoolOption(lineParts(3)),
          STATUS_ORIGINAL = lineParts(3),
          REF_OPERATOR_ID = lineParts(4),
          REF_CONTACT_PERSON_ID = lineParts(5),
          ORDER_TYPE = checkOrderType(lineParts(6)),
          TRANSACTION_DATE = parseDateTimeStampOption(lineParts(7)),
          TRANSACTION_DATE_ORIGINAL = lineParts(7),
          REF_PRODUCT_ID = lineParts(8),
          QUANTITY = parseLongRangeOption(lineParts(9)),
          QUANTITY_ORIGINAL = lineParts(9),
          ORDER_LINE_VALUE = parseBigDecimalOption(lineParts(10)), //TODO should be BigDecimal
          ORDER_LINE_VALUE_ORIGINAL = lineParts(10),
          ORDER_VALUE = parseBigDecimalOption(lineParts(11)),
          ORDER_VALUE_ORIGINAL = lineParts(11),
          WHOLESALER = lineParts(12),
          CAMPAIGN_CODE = lineParts(13),
          CAMPAIGN_NAME = lineParts(14),
          UNIT_PRICE = parseBigDecimalOption(lineParts(15)), //TODO should be BigDecimal
          UNIT_PRICE_ORIGINAL = lineParts(15),
          CURRENCY_CODE = lineParts(16),
          DATE_CREATED = parseDateTimeStampOption(lineParts(17)),
          DATE_MODIFIED = parseDateTimeStampOption(lineParts(18))
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
