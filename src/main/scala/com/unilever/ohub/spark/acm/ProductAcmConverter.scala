package com.unilever.ohub.spark.acm

import com.unilever.ohub.spark.generic.SparkFunctions.renameSparkCsvFileUsingHadoopFileSystem
import org.apache.spark.sql.SaveMode.Overwrite
import org.apache.spark.sql.SparkSession

object ProductAcmConverter extends App{
  if (args.length != 2) {
    println("specify INPUT_FILE OUTPUT_FILE")
    sys.exit(1)
  }

  val inputFile = args(0)
  val outputFile = args(1)

  println(s"Generating orders and orderlines ACM csv file from [$inputFile] to [$outputFile]")

  val spark = SparkSession
    .builder()
    .appName(this.getClass.getSimpleName)
    .getOrCreate()

  val startOfJob = System.currentTimeMillis()

  val productsInputDF = spark.read.parquet(inputFile)
  productsInputDF.createOrReplaceTempView("PDT_INPUT")

  val ufsProductsDF = spark.sql(
    s"""
      |select COUNTRY_CODE COUNTY_CODE,PRODUCT_NAME,PRODUCT_CONCAT_ID PRD_INTEGRATION_ID,EAN_CU EAN_CODE,MRDR MRDR_CODE,
      | date_format(DATE_CREATED,"yyyy-MM-dd HH:mm:ss") CREATED_AT,
      | date_format(DATE_MODIFIED,"yyyy-MM-dd HH:mm:ss") UPDATED_AT,
      | case when STATUS = true then 'N' else 'Y' end DELETE_FLAG
      |from PDT_INPUT
    """.stripMargin)
  ufsProductsDF.show(false)
  ufsProductsDF.coalesce(1).write.mode(Overwrite).option("encoding", "UTF-8").option("header", "true").option("delimiter","\u00B6").csv(outputFile)

  renameSparkCsvFileUsingHadoopFileSystem(spark,outputFile,"UFS_PRODUCTS")

}
