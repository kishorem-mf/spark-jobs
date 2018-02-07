package com.unilever.ohub.spark.acm

import java.util.UUID

import com.unilever.ohub.spark.generic.{ FileSystems, StringFunctions }
import org.apache.log4j.{ LogManager, Logger }
import org.apache.spark.sql.SaveMode.Overwrite
import org.apache.spark.sql.SparkSession

object OrderLineAcmConverter extends App with AcmConverterHelpers {
  implicit private val log: Logger = LogManager.getLogger(getClass)

  val (inputFile: String, outputFile: String, outputParquetFile: String) = FileSystems.getFileNames(
    args,
    "INPUT_FILE", "OUTPUT_FILE", "OUTPUT_PARQUET_FILE"
  )

  log.info(s"Generating orderlines ACM csv file from [$inputFile] to [$outputFile]")

  val spark = SparkSession
    .builder()
    .appName(this.getClass.getSimpleName)
    .getOrCreate()

  val startOfJob = System.currentTimeMillis()

  spark.sqlContext.udf.register("CLEAN", (s1: String) => StringFunctions.removeGenericStrangeChars(s1))
  spark.sqlContext.udf.register("UUID", (_: String) => UUID.randomUUID().toString)

  val ordersInputDF = spark.read.parquet(inputFile)
  ordersInputDF.createOrReplaceTempView("ORD_INPUT")

  val orderLinesDF = spark.sql(
    s"""
      |select UUID('') ORDERLINE_ID,ORDER_CONCAT_ID ORDER_ID,QUANTITY,
      | round(ORDER_LINE_VALUE,2) AMOUNT,
      | concat(COUNTRY_CODE,'~',SOURCE,'~',REF_PRODUCT_ID) PRD_INTEGRATION_ID,'' SAMPLE_ID
      |from ORD_INPUT
    """.stripMargin)
  .where("country_code = 'AU'") //  TODO remove country_code filter for production

  orderLinesDF.write.mode(Overwrite).format("parquet").save(outputParquetFile) /* COUNTRY is not an existing column, therefore no country partitioning */
  val ufsOrderLinesDF = spark.read.parquet(outputParquetFile).select("ORDERLINE_ID","ORDER_ID","QUANTITY","AMOUNT","PRD_INTEGRATION_ID","SAMPLE_ID")

  writeDataFrameToCSV(ufsOrderLinesDF, outputFile)

  finish(spark, outputFile, outputParquetFile, outputFileNewName = "UFS_ORDERLINES")
}
