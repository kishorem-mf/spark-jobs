package com.unilever.ohub.spark.acm

import com.unilever.ohub.spark.generic.FileSystems.removeFullDirectoryUsingHadoopFileSystem
import com.unilever.ohub.spark.generic.SparkFunctions.renameSparkCsvFileUsingHadoopFileSystem
import org.apache.spark.sql.SaveMode.Overwrite
import org.apache.spark.sql.SparkSession

object OrderAcmConverter extends App{
  if (args.length != 2) {
    println("specify INPUT_FILE OUTPUT_FILE")
    sys.exit(1)
  }

  val inputFile = args(0)
  val outputFile = args(1)
  val outputParquetFile = if(outputFile.endsWith(".csv")) outputFile.replace(".csv",".parquet") else outputFile

  println(s"Generating orders ACM csv file from [$inputFile] to [$outputFile]")

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

  ufsOrdersDF.coalesce(1).write.mode(Overwrite).option("encoding", "UTF-8").option("header", "true").option("delimiter","\u00B6").csv(outputFile)

  removeFullDirectoryUsingHadoopFileSystem(spark,outputParquetFile)
  renameSparkCsvFileUsingHadoopFileSystem(spark,outputFile,"UFS_ORDERS")
}
