package com.unilever.ohub.spark.acm

import com.unilever.ohub.spark.generic.FileSystems.removeFullDirectoryUsingHadoopFileSystem
import com.unilever.ohub.spark.generic.StringFunctions
import org.apache.spark.sql.SaveMode.Overwrite
import org.apache.spark.sql.SparkSession

object ContactPersonAcmConverter extends App{
  if (args.length != 2) {
    println("specify INPUT_FILE OUTPUT_FILE")
    sys.exit(1)
  }

  val inputFile = args(0)
  val outputFile = args(1)
  val outputParquetFile = if(outputFile.endsWith(".csv")) outputFile.replace(".csv",".parquet") else outputFile

  println(s"Generating contact person ACM csv file from [$inputFile] to [$outputFile]")

  val spark = SparkSession
    .builder()
    .appName(this.getClass.getSimpleName)
    .getOrCreate()

  import com.unilever.ohub.spark.generic.SparkFunctions._
  import org.apache.spark.sql.functions._

  val startOfJob = System.currentTimeMillis()

  spark.sqlContext.udf.register("CLEAN",(s1:String) => StringFunctions.removeGenericStrangeChars(s1 match {case null => null;case _ => s1}))
  spark.sqlContext.udf.register("CLEAN_NAMES",(s1:String,s2:String,b1:Boolean) => StringFunctions.fillLastNameOnlyWhenFirstEqualsLastName(s1 match {case null => null;case _ => s1},s2 match {case null => null;case _ => s2},b1))

  val contactPersonsInputDF = spark.read.parquet(inputFile)
    .select("OHUB_CONTACT_PERSON_ID","CONTACT_PERSON.*")
  contactPersonsInputDF.createOrReplaceTempView("CPN_INPUT")

//  Data Model: OPR_ORIG_INTEGRATION_ID can be misleading for Ohub 2.0 as this will contain the new OHUB_OPERATOR_ID and OPR_LNKD_INTEGRATION_ID will contain OPERATOR_CONCAT_ID
  val recipientsDF = spark.sql(
    s"""
      |select CONTACT_PERSON_CONCAT_ID CP_ORIG_INTEGRATION_ID,OHUB_CONTACT_PERSON_ID CP_LNKD_INTEGRATION_ID,REF_OPERATOR_ID OPR_ORIG_INTEGRATION_ID,'Y' GOLDEN_RECORD_FLAG,'' WEB_CONTACT_ID,
      | case when EM_OPT_OUT = true then 'Y' when EM_OPT_OUT = false then 'N' else 'U' end EMAIL_OPTOUT,
      | case when TM_OPT_IN = true then 'Y' when TM_OPT_IN = false then 'N' else 'U' end PHONE_OPTOUT,
      | case when FAX_OPT_OUT = true then 'Y' when FAX_OPT_OUT = false then 'N' else 'U' end FAX_OPTOUT,
      | case when MOB_OPT_OUT = true then 'Y' when MOB_OPT_OUT = false then 'N' else 'U' end MOBILE_OPTOUT,
      | case when DM_OPT_OUT = true then 'Y' when DM_OPT_OUT = false then 'N' else 'U' end DM_OPTOUT,
      | clean_names(FIRST_NAME,LAST_NAME,false) LAST_NAME,clean_names(FIRST_NAME,LAST_NAME,true) FIRST_NAME,'' MIDDLE_NAME,
      | TITLE,
      | case when GENDER = 'U' then '0' when GENDER = 'M' then '1' when GENDER = 'F' then '2' else '0' end GENDER,LANGUAGE_KEY LANGUAGE,EMAIL_ADDRESS,MOBILE_PHONE_NUMBER,PHONE_NUMBER,
      | FAX_NUMBER,
      | clean(STREET) STREET,
      | concat(clean(HOUSENUMBER),' ',clean(HOUSENUMBER_EXT)) HOUSENUMBER,
      | clean(ZIP_CODE) ZIPCODE,
      | clean(CITY) CITY,COUNTRY,
      | date_format(DATE_CREATED,"yyyy-MM-dd HH:mm:ss") DATE_CREATED,
      | date_format(DATE_MODIFIED,"yyyy-MM-dd HH:mm:ss") DATE_UPDATED,
      | date_format(BIRTH_DATE,"yyyy-MM-dd HH:mm:ss") DATE_OF_BIRTH,
      | case when PREFERRED_CONTACT = true then 'Y' when PREFERRED_CONTACT = false then 'N' else 'U' end PREFERRED,
      | FUNCTION ROLE,COUNTRY_CODE,SCM,
      | case when STATUS = true then '0' else '1' end DELETE_FLAG,
      | case when KEY_DECISION_MAKER = true then 'Y' when KEY_DECISION_MAKER = false then 'N' else 'U' end KEY_DECISION_MAKER,
      | case when EM_OPT_IN = true then 'Y' when EM_OPT_IN = false then 'N' else 'U' end OPT_IN,
      | date_format(EM_OPT_IN_DATE,"yyyy-MM-dd HH:mm:ss") OPT_IN_DATE,
      | case when EM_OPT_IN_CONFIRMED = true then 'Y' when EM_OPT_IN_CONFIRMED = false then 'N' else 'U' end CONFIRMED_OPT_IN,
      | date_format(EM_OPT_IN_CONFIRMED_DATE,"yyyy-MM-dd HH:mm:ss") CONFIRMED_OPT_IN_DATE,
      | case when MOB_OPT_IN = true then 'Y' when MOB_OPT_IN = false then 'N' else 'U' end MOB_OPT_IN,
      | date_format(MOB_OPT_IN_DATE,"yyyy-MM-dd HH:mm:ss") MOB_OPT_IN_DATE,
      | case when MOB_OPT_IN_CONFIRMED = true then 'Y' when MOB_OPT_IN_CONFIRMED = false then 'N' else 'U' end MOB_CONFIRMED_OPT_IN,
      | date_format(MOB_OPT_IN_CONFIRMED_DATE,"yyyy-MM-dd HH:mm:ss") MOB_CONFIRMED_OPT_IN_DATE,
      | '' MOB_OPT_OUT_DATE,
      | FIRST_NAME ORG_FIRST_NAME,LAST_NAME ORG_LAST_NAME,EMAIL_ADDRESS_ORIGINAL ORG_EMAIL_ADDRESS,PHONE_NUMBER_ORIGINAL ORG_FIXED_PHONE_NUMBER,PHONE_NUMBER_ORIGINAL ORG_MOBILE_PHONE_NUMBER,FAX_NUMBER ORG_FAX_NUMBER
      |from CPN_INPUT
    """.stripMargin)
    .where("country_code = 'AU'") //  TODO remove country_code filter for production

  recipientsDF.write.mode(Overwrite).partitionBy("COUNTRY_CODE").format("parquet").save(outputParquetFile)
  val ufsRecipientsDF = spark.read.parquet(outputParquetFile).select("CP_ORIG_INTEGRATION_ID","CP_LNKD_INTEGRATION_ID","OPR_ORIG_INTEGRATION_ID","GOLDEN_RECORD_FLAG","WEB_CONTACT_ID","EMAIL_OPTOUT","PHONE_OPTOUT","FAX_OPTOUT","MOBILE_OPTOUT","DM_OPTOUT","LAST_NAME","FIRST_NAME","MIDDLE_NAME","TITLE","GENDER","LANGUAGE","EMAIL_ADDRESS","MOBILE_PHONE_NUMBER","PHONE_NUMBER","FAX_NUMBER","STREET","HOUSENUMBER","ZIPCODE","CITY","COUNTRY","DATE_CREATED","DATE_UPDATED","DATE_OF_BIRTH","PREFERRED","ROLE","COUNTRY_CODE","SCM","DELETE_FLAG","KEY_DECISION_MAKER","OPT_IN","OPT_IN_DATE","CONFIRMED_OPT_IN","CONFIRMED_OPT_IN_DATE","MOB_OPT_IN","MOB_OPT_IN_DATE","MOB_CONFIRMED_OPT_IN","MOB_CONFIRMED_OPT_IN_DATE","MOB_OPT_OUT_DATE","ORG_FIRST_NAME","ORG_LAST_NAME","ORG_EMAIL_ADDRESS","ORG_FIXED_PHONE_NUMBER","ORG_MOBILE_PHONE_NUMBER","ORG_FAX_NUMBER")

  ufsRecipientsDF.coalesce(1).write.mode(Overwrite).option("encoding", "UTF-8").option("header", "true").option("delimiter","\u00B6").csv(outputFile)

  removeFullDirectoryUsingHadoopFileSystem(spark,outputParquetFile)
  renameSparkCsvFileUsingHadoopFileSystem(spark,outputFile,"UFS_RECIPIENTS")
}
