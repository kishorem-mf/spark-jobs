package com.unilever.ohub.spark.acm

import java.util.UUID

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

  println(s"Generating orders and orderlines ACM csv file from [$inputFile] to [$outputFile]")

  val spark = SparkSession
    .builder()
    .appName(this.getClass.getSimpleName)
    .getOrCreate()

  val startOfJob = System.currentTimeMillis()

  spark.sqlContext.udf.register("CLEAN",(s1:String) => StringFunctions.removeGenericStrangeChars(s1 match {case null => null;case _ => s1}))
  spark.sqlContext.udf.register("UUID",(s1:String) => s1 match {case null => null;case _ => UUID.randomUUID().toString})

  val ordersInputDF = spark.read.parquet(inputFile)
  ordersInputDF.createOrReplaceTempView("ORD_INPUT")

//  TODO remove country_code filter for production
  val ufsOrderLinesDF = spark.sql(
    s"""
      |select UUID('') ORDERLINE_ID,ORDER_CONCAT_ID ORDER_ID,QUANTITY,
      | round(ORDER_LINE_VALUE,2) AMOUNT,
      | concat(COUNTRY_CODE,'~',SOURCE,'~',REF_PRODUCT_ID) PRD_INTEGRATION_ID,'' SAMPLE_ID
      |from ORD_INPUT
    """.stripMargin)
  .where("country_code = 'DK'")
  ufsOrderLinesDF.show(false)
  ufsOrderLinesDF.coalesce(1).write.mode(Overwrite).option("encoding", "UTF-8").option("header", "true").option("delimiter","\u00B6").csv(outputFile)

  renameSparkCsvFileUsingHadoopFileSystem(spark,outputFile,"UFS_ORDERLINES")
}
