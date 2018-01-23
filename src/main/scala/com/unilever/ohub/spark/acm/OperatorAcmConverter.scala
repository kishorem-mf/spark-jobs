package com.unilever.ohub.spark.acm
import org.apache.spark.sql.SparkSession
import com.unilever.ohub.spark.generic.StringFunctions
import org.apache.hadoop.fs.FileSystem

object OperatorAcmConverter extends App{
  if (args.length != 2) {
    println("specify INPUT_FILE OUTPUT_FILE")
    sys.exit(1)
  }

  val inputFile = args(0)
  val outputFile = args(1)

  println(s"Generating operator parquet from [$inputFile] to [$outputFile]")

  val spark = SparkSession
    .builder()
    .appName(this.getClass.getSimpleName)
    .getOrCreate()

  import spark.implicits._
  import org.apache.spark.sql.functions._
  import com.unilever.ohub.spark.generic.SparkFunctions._

  val startOfJob = System.currentTimeMillis()

  val channelMappingDF = readJdbcTable(spark,dbTable = "channel_mapping")
  val channelReferencesDF = readJdbcTable(spark,dbTable = "channel_references")
  val allChannelMappingDF = channelMappingDF.join(channelReferencesDF,col("channel_reference_fk") === col("channel_reference_id"),"left")

  spark.sqlContext.udf.register("CLEAN",(s1:String) => StringFunctions.removeGenericStrangeChars(s1 match {case null => null;case _ => s1}))
  val operatorsInputDF = spark.read.parquet(inputFile)
  allChannelMappingDF.createOrReplaceTempView("CHANNEL_MAPPING")
  operatorsInputDF.createOrReplaceTempView("OPR_INPUT")

//  TODO make sure OPR_LNKD_INTEGRATION_ID = filled with new group id + set GOLDEN_RECORD_FLAG to Y if golden record
  val operatorFieldsParquetDF = spark.sql(
    """
      |select OPERATOR_CONCAT_ID OPR_ORIG_INTEGRATION_ID,'' OPR_LNKD_INTEGRATION_ID,'' GOLDEN_RECORD_FLAG,COUNTRY_CODE,
      | clean(NAME) NAME,CHANNEL,SUB_CHANNEL,'' ROUTE_TO_MARKET,REGION,OTM,
      | clean(DISTRIBUTOR_NAME) PREFERRED_PARTNER,STREET,HOUSENUMBER HOUSE_NUMBER,ZIP_CODE ZIPCODE,
      | clean(CITY) CITY,COUNTRY,
      | round(AVG_PRICE,2) AVERAGE_SELLING_PRICE,NR_OF_DISHES NUMBER_OF_COVERS,
      | case when 52 - WEEKS_CLOSED < 0 then null else 52 - WEEKS_CLOSED end NUMBER_OF_WEEKS_OPEN,DAYS_OPEN NUMBER_OF_DAYS_OPEN,CONVENIENCE_LEVEL,SALES_REP RESPONSIBLE_EMPLOYEE,NPS_POTENTIAL,'' CAM_KEY,'' CAM_TEXT,'' CHANNEL_KEY,'' CHANNEL_TEXT,CHAIN_ID CHAIN_KNOTEN,
      | clean(CHAIN_NAME) CHAIN_NAME,'' CUST_SUB_SEG_EXT,'' CUST_SEG_EXT,'' CUST_SEG_KEY_EXT,'' CUST_GRP_EXT,'' PARENT_SEGMENT,DATE_CREATED,DATE_MODIFIED DATE_UPDATED,
      | case when STATUS = true then 'N' else 'Y' end DELETE_FLAG,DISTRIBUTOR_CUSTOMER_NR WHOLESALER_OPERATOR_ID,
      | case when PRIVATE_HOUSEHOLD = false then 'N' else 'Y' end PRIVATE_HOUSEHOLD,VAT_NUMBER VAT,
      | case when OPEN_ON_MONDAY = false then 'N' else 'Y' end OPEN_ON_MONDAY,
      | case when OPEN_ON_TUESDAY = false then 'N' else 'Y' end OPEN_ON_TUESDAY,
      | case when OPEN_ON_WEDNESDAY = false then 'N' else 'Y' end OPEN_ON_WEDNESDAY,
      | case when OPEN_ON_THURSDAY = false then 'N' else 'Y' end OPEN_ON_THURSDAY,
      | case when OPEN_ON_FRIDAY = false then 'N' else 'Y' end OPEN_ON_FRIDAY,
      | case when OPEN_ON_SATURDAY = false then 'N' else 'Y' end OPEN_ON_SATURDAY,
      | case when OPEN_ON_SUNDAY = false then 'N' else 'Y' end OPEN_ON_SUNDAY,
      | clean(KITCHEN_TYPE) KITCHEN_TYPE,
      | '' LOCAL_CHANNEL,'' CHANNEL_USAGE,'' SOCIAL_COMMERCIAL,'' STRATEGIC_CHANNEL,'' GLOBAL_CHANNEL,'' GLOBAL_SUBCHANNEL
      |from OPR_INPUT
    """.stripMargin)
  operatorFieldsParquetDF.createOrReplaceTempView("OPR")

  val ufsOperatorsDF = spark.sql(
    """
      |select OPR.OPR_ORIG_INTEGRATION_ID,OPR.OPR_LNKD_INTEGRATION_ID,OPR.GOLDEN_RECORD_FLAG,OPR.COUNTRY_CODE,OPR.NAME,OPR.CHANNEL,OPR.SUB_CHANNEL,OPR.ROUTE_TO_MARKET,OPR.REGION,OPR.OTM,OPR.PREFERRED_PARTNER,OPR.STREET,OPR.HOUSE_NUMBER,OPR.ZIPCODE,OPR.CITY,OPR.COUNTRY,OPR.AVERAGE_SELLING_PRICE,OPR.NUMBER_OF_COVERS,OPR.NUMBER_OF_WEEKS_OPEN,OPR.NUMBER_OF_DAYS_OPEN,OPR.CONVENIENCE_LEVEL,OPR.RESPONSIBLE_EMPLOYEE,OPR.NPS_POTENTIAL,OPR.CAM_KEY,OPR.CAM_TEXT,OPR.CHANNEL_KEY,OPR.CHANNEL_TEXT,OPR.CHAIN_KNOTEN,OPR.CHAIN_NAME,OPR.CUST_SUB_SEG_EXT,OPR.CUST_SEG_EXT,OPR.CUST_SEG_KEY_EXT,OPR.CUST_GRP_EXT,OPR.PARENT_SEGMENT,OPR.DATE_CREATED,OPR.DATE_UPDATED,OPR.DELETE_FLAG,OPR.WHOLESALER_OPERATOR_ID,OPR.PRIVATE_HOUSEHOLD,OPR.VAT,OPR.OPEN_ON_MONDAY,OPR.OPEN_ON_TUESDAY,OPR.OPEN_ON_WEDNESDAY,OPR.OPEN_ON_THURSDAY,OPR.OPEN_ON_FRIDAY,OPR.OPEN_ON_SATURDAY,OPR.OPEN_ON_SUNDAY,OPR.KITCHEN_TYPE,MPG.LOCAL_CHANNEL,MPG.CHANNEL_USAGE,MPG.SOCIAL_COMMERCIAL,MPG.STRATEGIC_CHANNEL,MPG.GLOBAL_CHANNEL,MPG.GLOBAL_SUBCHANNEL
      |from OPR
      |left join CHANNEL_MAPPING MPG
      | on OPR.CHANNEL = MPG.ORIGINAL_CHANNEL
    """.stripMargin)

  ufsOperatorsDF.coalesce(1).write.option("encoding", "UTF-8").option("header", "true").option("delimiter","\u00B6").csv(outputFile)

//  Rename file
  import org.apache.hadoop.fs.{Path => hadoopPath}
  val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
  val originalPath = new hadoopPath(s"$outputFile/part*")
  val file = fs.globStatus(originalPath)(0).getPath
  val dateTime = java.time.LocalDateTime.now().toString
  val fileTimeStamp = s"${dateTime.substring(0,4)}${dateTime.substring(5,7)}${dateTime.substring(8,10)}${dateTime.substring(11,13)}${dateTime.substring(14,16)}${dateTime.substring(17,19)}"

  fs.rename(new hadoopPath(s"${file.getParent}/${file.getName}"), new hadoopPath(s"${file.getParent.getParent}/UFS_OPERATORS_$fileTimeStamp.csv"))
  fs.delete(new hadoopPath(s"${file.getParent}"), true)
}
