package com.unilever.ohub.spark.matching

import java.io.File

import org.apache.spark.storage.StorageLevel

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
  val cpn = spark.read.parquet(inputFile)
  import spark.implicits._
  cpn.createOrReplaceTempView("cpn_full")
  val countryList = cpn.select("country_code").groupBy("country_code").count().orderBy("count").collect().map(row => row(0).toString).toList
  createContactMatchGroupsPerCountry(outputFolder,countryList)
  def createContactMatchGroupsPerCountry(outputFolder:File,countryList:List[String]): Unit = {
    for(i <- countryList.indices) loopPerCountry(countryList(i))

    def loopPerCountry(countryCode:String): Unit = {
      val cpnPart = spark.sql(
        """
          |select distinct country_code,concat(country_code,'~',source,'~',ref_contact_person_id) id,first_name,last_name,both_names_cleansed,zip_code,zip_code_cleansed,street,street_cleansed,city,city_cleansed,substring(both_names_cleansed,1,3) name_block,substring(street_cleansed,1,3) street_block,email_address,mobile_phone_number
          |from cpn_full
          |where (both_names_cleansed is not null or mobile_phone_number is not null)
        """.stripMargin).where("country_code = '".concat(countryCode).concat("'"))
      cpnPart.createOrReplaceTempView("cpn_part")
      cpnPart.persist(StorageLevel.MEMORY_AND_DISK)

      val cpnUniqueEmail = spark.sql(
        """
          |select a.country_code,min(a.id) source_id,b.id target_id
          |from cpn_part a
          |inner join cpn_part b
          | on a.email_address = b.email_address
          | and a.id < b.id
          | and a.email_address is not null and b.email_address is not null
          | and a.country_code = b.country_code
          |group by a.country_code,b.id
        """.stripMargin)
      cpnUniqueEmail.createOrReplaceTempView("cpn_unique_email")
      cpnUniqueEmail.persist(StorageLevel.MEMORY_AND_DISK)

      val cpnUniqueMobile = spark.sql(
        """
          |select a.country_code,min(a.id) source_id,b.id target_id
          |from cpn_part a
          |inner join cpn_part b
          | on a.mobile_phone_number = b.mobile_phone_number
          | and a.id < b.id
          | and a.email_address is null and b.email_address is null
          | and a.mobile_phone_number is not null and b.mobile_phone_number is not null
          | and a.country_code = b.country_code
          |group by a.country_code,b.id
        """.stripMargin)
      cpnUniqueMobile.createOrReplaceTempView("cpn_unique_mobile")
      cpnUniqueMobile.persist(StorageLevel.MEMORY_AND_DISK)

      val cpnMatchGroups = cpnUniqueEmail.union(cpnUniqueMobile).distinct().sort("country_code", "source_id")
      cpnMatchGroups.persist(StorageLevel.MEMORY_AND_DISK)
    }
  }
}
