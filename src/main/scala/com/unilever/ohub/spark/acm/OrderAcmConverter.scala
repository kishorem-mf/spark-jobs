package com.unilever.ohub.spark.acm

import org.apache.log4j.LogManager
import org.apache.spark.sql.SaveMode.Overwrite
import org.apache.spark.sql.SparkSession

object OrderAcmConverter extends App with AcmConverterHelpers {
  protected val log = LogManager.getLogger(getClass)

  val (inputFile, outputFile, outputParquetFile) = getFileNames(args)

  log.debug(s"Generating orders ACM csv file from [$inputFile] to [$outputFile]")

  val spark = SparkSession
    .builder()
    .appName(this.getClass.getSimpleName)
    .getOrCreate()

  val startOfJob = System.currentTimeMillis()

  val ordersInputDF = spark.read.parquet(inputFile)

  ordersInputDF.createOrReplaceTempView("ORD_INPUT")

  val ordersDF = spark.sql(
    s"""
       |select distinct ORDER_CONCAT_ID ORDER_ID,COUNTRY_CODE,ORDER_TYPE,REF_CONTACT_PERSON_ID CP_LNKD_INTEGRATION_ID,REF_OPERATOR_ID OPR_LNKD_INTEGRATION_ID,CAMPAIGN_CODE,CAMPAIGN_NAME,WHOLESALER,'' ORDER_TOKEN,
       | date_format(TRANSACTION_DATE,"yyyy-MM-dd HH:mm:ss") TRANSACTION_DATE,
       | round(ORDER_VALUE,2) ORDER_AMOUNT,CURRENCY_CODE ORDER_AMOUNT_CURRENCY_CODE,'' DELIVERY_STREET,'' DELIVERY_HOUSENUMBER,'' DELIVERY_ZIPCODE,'' DELIVERY_CITY,'' DELIVERY_STATE,'' DELIVERY_COUNTRY,'' DELIVERY_PHONE
       |from ORD_INPUT
    """.stripMargin)
  .where("country_code = 'AU'") //  TODO remove country_code filter for production

  ordersDF.write.mode(Overwrite).partitionBy("COUNTRY_CODE").format("parquet").save(outputParquetFile)
  val ufsOrdersDF = spark.read.parquet(outputParquetFile).select("ORDER_ID","COUNTRY_CODE","ORDER_TYPE","CP_LNKD_INTEGRATION_ID","OPR_LNKD_INTEGRATION_ID","CAMPAIGN_CODE","CAMPAIGN_NAME","WHOLESALER","ORDER_TOKEN","TRANSACTION_DATE","ORDER_AMOUNT","ORDER_AMOUNT_CURRENCY_CODE","DELIVERY_STREET","DELIVERY_HOUSENUMBER","DELIVERY_ZIPCODE","DELIVERY_CITY","DELIVERY_STATE","DELIVERY_COUNTRY","DELIVERY_PHONE")

  writeDataFrameToCSV(ufsOrdersDF, outputFile)

  finish(spark, outputFile, outputParquetFile, outputFileNewName = "UFS_ORDERS")
}
