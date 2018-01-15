package com.unilever.ohub.spark.matching

import java.io.File

object ContactPersonMatching extends App {
  if (args.length != 2) {
    println("specify INPUT_FILE OUTPUT_FOLDER")
    sys.exit(1)
  }

  val inputFile = args(0)
  val outputFolder = new File(args(1))

  import org.apache.spark.sql.SparkSession
  import com.unilever.ohub.spark.tsv2parquet.StringFunctions
  val spark = SparkSession.builder().appName("Contactperson matching").getOrCreate()
  spark.sqlContext.udf.register("SIMILARITY",(s1:String,s2:String) => StringFunctions.getFastSimilarity(s1 match {case null => null;case _ => s1.toCharArray},s2 match {case null => null;case _ => s2.toCharArray}))
  val contactsDF1 = spark.read.parquet(inputFile)
  import spark.implicits._
  contactsDF1.createOrReplaceTempView("contacts1")
  val countryList = contactsDF1.select("country_code").groupBy("country_code").count().orderBy("count").collect().map(row => row(0).toString).toList
  createContactMatchGroupsPerCountry(outputFolder,countryList)
  def createContactMatchGroupsPerCountry(outputFolder:File,countryList:List[String]): Unit = {

  }
}
