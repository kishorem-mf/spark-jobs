package com.unilever.ohub.spark.matching

import org.apache.spark.sql.SaveMode.Overwrite
import org.apache.spark.storage.StorageLevel

object ContactPersonMatching extends App {
  if (args.length != 2) {
    println("specify INPUT_FILE OUTPUT_FILE")
    sys.exit(1)
  }

  val inputFile = args(0)
  val outputFile = args(1)

  println(s"Generating parquet from [$inputFile] to [$outputFile]")

  import org.apache.spark.sql.SparkSession
  val spark = SparkSession.builder().appName("ContactPerson matching").getOrCreate()
  val cpn = spark.read.parquet(inputFile)
  cpn.createOrReplaceTempView("cpn_full")
  import org.apache.spark.sql.functions._
//  TODO delete country filter
  val cpnPart = spark.sql(
    """
      |select distinct country_code,concat(country_code,'~',source,'~',ref_contact_person_id) id,first_name,last_name,both_names_cleansed,zip_code,zip_code_cleansed,street,street_cleansed,city,city_cleansed,substring(both_names_cleansed,1,3) name_block,substring(street_cleansed,1,3) street_block,email_address,mobile_phone_number
      |from cpn_full
      |where (both_names_cleansed is not null or mobile_phone_number is not null)
    """.stripMargin)
    .where("country_code = 'DK'")
  cpnPart.createOrReplaceTempView("cpn_part")
  cpnPart.repartition(col("country_code")).persist(StorageLevel.MEMORY_AND_DISK)

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
  cpnUniqueEmail.repartition(col("country_code")).persist(StorageLevel.MEMORY_AND_DISK)

  val cpnUniqueMobile = spark.sql(
    """
      |select a.country_code,min(a.id) source_id,b.id target_id
      |from cpn_part a
      |inner join cpn_part b
      | on a.mobile_phone_number = b.mobile_phone_number
      | and a.id < b.id
      | and a.email_address is null and b.email_address is null
      | and a.country_code = b.country_code
      |group by a.country_code,b.id
    """.stripMargin)
  cpnUniqueMobile.createOrReplaceTempView("cpn_unique_mobile")
  cpnUniqueMobile.repartition(col("country_code")).persist(StorageLevel.MEMORY_AND_DISK)

  val cpnMatchGroups = cpnUniqueEmail.union(cpnUniqueMobile).distinct().sort("country_code","source_id")
  cpnMatchGroups.createOrReplaceTempView("cpn_match_groups")
  cpnMatchGroups.repartition(col("country_code")).persist(StorageLevel.MEMORY_AND_DISK)

  val cpnMatchesOne = spark.sql(
    """
      |select distinct mth.country_code,mth.source_id,mth.target_id,prt.both_names_cleansed source_names,'' target_names,prt.email_address source_email,'' target_email ,prt.mobile_phone_number source_mobile,'' target_mobile
      |from cpn_match_groups mth
      |inner join cpn_part prt
      | on mth.source_id = prt.id
      | and mth.country_code = prt.country_code
    """.stripMargin)
  cpnMatchesOne.createOrReplaceTempView("cpn_matches_one")
  cpnMatchesOne.repartition(col("country_code")).persist(StorageLevel.MEMORY_AND_DISK)

  val cpnMatches = spark.sql(
    """
      |select distinct mth.country_code,mth.source_id,mth.target_id,mth.source_names,prt.both_names_cleansed target_names,mth.source_email,prt.email_address target_email,mth.source_mobile,prt.mobile_phone_number target_mobile
      |from cpn_matches_one mth
      |inner join cpn_part prt
      | on mth.target_id = prt.id
      | and mth.country_code = prt.country_code
      |order by source_id
    """.stripMargin)
  cpnMatches.createOrReplaceTempView("cpn_matches")

  cpnMatches.write.mode(Overwrite).partitionBy("COUNTRY_CODE").format("parquet").save(outputFile)
}
