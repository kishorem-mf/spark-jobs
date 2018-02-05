package com.unilever.ohub.spark.acm

import com.unilever.ohub.spark.generic.FileSystems
import org.apache.log4j.{ LogManager, Logger }
import org.apache.spark.sql.SaveMode.Overwrite
import org.apache.spark.sql.SparkSession

object ProductAcmConverter extends App with AcmConverterHelpers {
  implicit private val log: Logger = LogManager.getLogger(getClass)

  val (inputFile, outputFile, outputParquetFile) = FileSystems.getInputOutputOutputParquetFileNames(args)

  log.info(s"Generating products ACM csv file from [$inputFile] to [$outputFile]")

  val spark = SparkSession
    .builder()
    .appName(this.getClass.getSimpleName)
    .getOrCreate()

  val startOfJob = System.currentTimeMillis()

  val productsInputDF = spark.read.parquet(inputFile)
  productsInputDF.createOrReplaceTempView("PDT_INPUT")

  val productsDF = spark.sql(
    s"""
      |select COUNTRY_CODE COUNTY_CODE,PRODUCT_NAME,PRODUCT_CONCAT_ID PRD_INTEGRATION_ID,EAN_CU EAN_CODE,MRDR MRDR_CODE,
      | date_format(DATE_CREATED,"yyyy-MM-dd HH:mm:ss") CREATED_AT,
      | date_format(DATE_MODIFIED,"yyyy-MM-dd HH:mm:ss") UPDATED_AT,
      | case when STATUS = true then 'N' else 'Y' end DELETE_FLAG
      |from PDT_INPUT
    """.stripMargin)
      .where("county_code = 'AU'") //  TODO remove country_code filter for production

  productsDF.show(false)
  productsDF.write.mode(Overwrite).partitionBy("COUNTY_CODE").format("parquet").save(outputParquetFile)
  val ufsProductsDF = spark.read.parquet(outputParquetFile).select("COUNTY_CODE","PRODUCT_NAME","PRD_INTEGRATION_ID","EAN_CODE","MRDR_CODE","CREATED_AT","UPDATED_AT","DELETE_FLAG")

  writeDataFrameToCSV(ufsProductsDF, outputFile)

  finish(spark, outputFile, outputParquetFile, outputFileNewName = "UFS_PRODUCTS")
}
