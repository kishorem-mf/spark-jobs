package com.unilever.ohub.spark.acm

import java.util.UUID

import com.unilever.ohub.spark.generic.FileSystems.removeFullDirectoryUsingHadoopFileSystem
import com.unilever.ohub.spark.generic.SparkFunctions.renameSparkCsvFileUsingHadoopFileSystem
import com.unilever.ohub.spark.generic.StringFunctions
import org.apache.spark.sql.SaveMode.Overwrite
import org.apache.spark.sql.SparkSession

object OrderLineAcmConverter extends App{
  if (args.length != 2) {
    println("specify INPUT_FILE OUTPUT_FILE")
    sys.exit(1)
  }

  val inputFile = args(0)
  val outputFile = args(1)
  val outputParquetFile = if(outputFile.endsWith(".csv")) outputFile.replace(".csv",".parquet") else outputFile

  println(s"Generating orderlines ACM csv file from [$inputFile] to [$outputFile]")

  val spark = SparkSession
    .builder()
    .appName(this.getClass.getSimpleName)
    .getOrCreate()

  val startOfJob = System.currentTimeMillis()

  spark.sqlContext.udf.register("CLEAN",(s1:String) => StringFunctions.removeGenericStrangeChars(s1 match {case null => null;case _ => s1}))
  spark.sqlContext.udf.register("UUID",(s1:String) => s1 match {case null => null;case _ => UUID.randomUUID().toString})

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

  ufsOrderLinesDF.coalesce(1).write.mode(Overwrite).option("encoding", "UTF-8").option("header", "true").option("delimiter","\u00B6").csv(outputFile)

  removeFullDirectoryUsingHadoopFileSystem(spark,outputParquetFile)
  renameSparkCsvFileUsingHadoopFileSystem(spark,outputFile,"UFS_ORDERLINES")
}
