// Databricks notebook source
// MAGIC %md
// MAGIC ### Common methods initializing

// COMMAND ----------

// MAGIC %run ./BaseInsightExporter

// COMMAND ----------

import org.apache.spark.sql.functions.input_file_name
import org.apache.spark.sql.functions.{concat,split, lit}
import org.apache.spark.sql.functions._

// COMMAND ----------

import org.apache.spark.sql.functions.{concat,split, lit}
var incomingData_op = spark
        .read
        .option("header", true)
        .option("sep", ";")
        .option("inferSchema", value = true)
        .csv("dbfs:/mnt/inbound/incoming/*/*OPERATORS*.csv")
        .select("REF_OPERATOR_ID","Source")

var incomingData_cp = spark
        .read
        .option("header", true)
        .option("sep", ";")
        .option("inferSchema", value = true)
        .csv("dbfs:/mnt/inbound/incoming/*/*CONTACTPERSONS*.csv")
        .select("REF_OPERATOR_ID","Source")

// COMMAND ----------

var driver = "org.postgresql.Driver"
var url = "jdbc:postgresql://ohub2-db-prod.postgres.database.azure.com:5432/ohub2?sslmode=require"
var query = "select FU.file_name as filename,ad.country,FU.status,FU.timestamp::date, CASE WHEN char_length(FU.user_name)-char_length(replace(FU.user_name,'-',''))=4 THEN spn.spnname ELSE FU.user_name END AS UserName FROM inbound.AUDIT_TRAILS AS FU LEFT JOIN inbound.adgroupusers AS ad ON FU.user_name = ad.username LEFT JOIN inbound.serviceprincipals as spn ON  spn.spnid = FU.user_name where (FU.status='COMPLETED' OR FU.status='FAILED') GROUP BY filename, ad.country, spn.spnname"

var user = "ohub2reader@ohub2-db-prod"
var password = "J2YneDDSajt3Mkks"

var df = spark.read.format("jdbc")
  .option("driver", driver)
  .option("url", url)
  .option("query", query)
  .option("user", user)
  .option("password", password)
  .load()
df=df.select("filename","country","timestamp","username")

// COMMAND ----------

var incomingData_cp_with_filename=incomingData_cp.withColumn("filename",split(input_file_name,"/")(5))

var incoming_op=incomingData_op.select("REF_OPERATOR_ID")
var incoming_cp=incomingData_cp.select("REF_OPERATOR_ID")

var with_link_cp_op=incoming_cp.intersect(incoming_op)
with_link_cp_op=with_link_cp_op.dropDuplicates()

with_link_cp_op=with_link_cp_op.withColumn("isLinked",lit(1))
with_link_cp_op=with_link_cp_op.withColumn("isNotLinked",lit(0))

var with_link_cp_op_final=with_link_cp_op.join(incomingData_cp_with_filename,Seq("REF_OPERATOR_ID"),"inner")
var agg_sum_linked_cp=with_link_cp_op_final.groupBy("filename").agg(sum("isLinked") as "isLinked",sum("isNotLinked") as "isNotLinked",max("Source") as "Source") 
agg_sum_linked_cp=agg_sum_linked_cp.withColumn("Entity",lit("CONTACTPERSONS"))

var with_no_link_cp_op=incoming_cp.except(incoming_op)
with_no_link_cp_op=with_no_link_cp_op.withColumn("isLinked",lit(0))
with_no_link_cp_op=with_no_link_cp_op.withColumn("isNotLinked",lit(1))
with_no_link_cp_op=with_no_link_cp_op.dropDuplicates()

var with_no_link_cp_op_final=with_no_link_cp_op.join(incomingData_cp_with_filename,Seq("REF_OPERATOR_ID"),"inner")
var unionOfLinkedandNotLinked=with_link_cp_op_final.unionByName(with_no_link_cp_op_final)
var agg_sum_no_linked_cp=unionOfLinkedandNotLinked.groupBy("filename").agg(sum("isLinked") as "isLinked",sum("isNotLinked") as "isNotLinked",max("Source") as "Source")
agg_sum_no_linked_cp=agg_sum_no_linked_cp.withColumn("Entity",lit("CONTACTPERSONS with OPERATORS"))
agg_sum_no_linked_cp=agg_sum_no_linked_cp.withColumn("filename", split(agg_sum_no_linked_cp.col("filename"),"\\.")(0))

// COMMAND ----------

var contactpersons=(agg_sum_no_linked_cp.join(df,Seq("filename"),"left"))

// COMMAND ----------

var incomingData_ol = spark
        .read
        .option("header", true)
        .option("sep", ";")
       // .option("escape","\"")
        .option("inferSchema", value = true)
        .csv("dbfs:/mnt/inbound/incoming/*/*ORDERLINES*.csv")
        .select("REF_ORDER_ID","SOURCE")

var incomingData_or = spark
        .read
        .option("header", true)
        .option("sep", ";")
       // .option("escape","\"")
        .option("inferSchema", value = true)
        .csv("dbfs:/mnt/inbound/incoming/*/*ORDERS*.csv")
        .select("REF_ORDER_ID","SOURCE")

// COMMAND ----------

var incomingData_ol_with_filename=incomingData_ol.withColumn("filename", split(input_file_name,"/")(5))

// COMMAND ----------

var incoming_or=incomingData_or.select("REF_ORDER_ID")
var incoming_ol=incomingData_ol.select("REF_ORDER_ID")
var with_link_or_ol=incoming_ol.intersect(incoming_or)
with_link_or_ol=with_link_or_ol.dropDuplicates()
with_link_or_ol=with_link_or_ol.withColumn("isLinked",lit(1))
with_link_or_ol=with_link_or_ol.withColumn("isNotLinked",lit(0))
var with_link_or_ol_final=with_link_or_ol.join(incomingData_ol_with_filename,Seq("REF_ORDER_ID"),"inner")

var with_no_link_or_ol=incoming_ol.except(incoming_or)
with_no_link_or_ol=with_no_link_or_ol.withColumn("isLinked",lit(0))
with_no_link_or_ol=with_no_link_or_ol.withColumn("isNotLinked",lit(1))
with_no_link_or_ol=with_no_link_or_ol.dropDuplicates()
var with_no_link_or_ol_final=with_no_link_or_ol.join(incomingData_ol_with_filename,Seq("REF_ORDER_ID"),"inner")

var unionOfLinkedandNotLinked_ol=with_link_or_ol_final.unionByName(with_no_link_or_ol_final)
var agg_sum_no_linked_ol=unionOfLinkedandNotLinked_ol.groupBy("filename").agg(sum("isLinked") as "isLinked",sum("isNotLinked") as "isNotLinked",max("Source") as "Source")
agg_sum_no_linked_ol=agg_sum_no_linked_ol.withColumn("ENTITY",lit("ORDERLINES with ORDERS"))

// COMMAND ----------

agg_sum_no_linked_ol=agg_sum_no_linked_ol.withColumn("filename", split(agg_sum_no_linked_ol.col("filename"),"\\.")(0))
var orderlines_with_orders=agg_sum_no_linked_ol.join(df,Seq("filename"),"left")

// COMMAND ----------

var incomingData_pr = spark
        .read
        .option("header", true)
        .option("sep", ";")
       // .option("escape","\"")
        .option("inferSchema", value = true)
        .csv("dbfs:/mnt/inbound/incoming/*/*PRODUCT*.csv")
        .select("REF_PRODUCT_ID","SOURCE")

// COMMAND ----------

var incomingData_ol_pr = spark
        .read
        .option("header", true)
        .option("sep", ";")
       // .option("escape","\"")
        .option("inferSchema", value = true)
        .csv("dbfs:/mnt/inbound/incoming/*/*ORDERLINES*.csv")
        .select("REF_PRODUCT_ID","SOURCE")

// COMMAND ----------

var incomingData_pr_with_filename=incomingData_pr.withColumn("filename", split(input_file_name,"/")(5))

// COMMAND ----------

var incoming_pr=incomingData_pr.select("REF_PRODUCT_ID")
var incoming_ol_pr=incomingData_ol_pr.select("REF_PRODUCT_ID")

var with_link_ol_pr=incoming_ol_pr.intersect(incoming_pr)
with_link_ol_pr=with_link_ol_pr.dropDuplicates()
with_link_ol_pr=with_link_ol_pr.withColumn("isLinked",lit(1))
with_link_ol_pr=with_link_ol_pr.withColumn("isNotLinked",lit(0))
var with_link_pr_ol_final=with_link_ol_pr.join(incomingData_pr_with_filename,Seq("REF_PRODUCT_ID"),"inner")


// COMMAND ----------


var with_no_link_pr_ol=incoming_ol_pr.except(incoming_pr)


// COMMAND ----------

with_no_link_pr_ol=with_no_link_pr_ol.withColumn("isLinked",lit(0))
with_no_link_pr_ol=with_no_link_pr_ol.withColumn("isNotLinked",lit(1))
with_no_link_pr_ol=with_no_link_pr_ol.dropDuplicates()


// COMMAND ----------

var with_no_link_pr_ol_final=with_no_link_pr_ol.join(incomingData_pr_with_filename,Seq("REF_PRODUCT_ID"),"inner")


// COMMAND ----------

var unionOfLinkedandNotLinked_ol=with_link_pr_ol_final.unionByName(with_no_link_pr_ol_final)
var agg_sum_no_linked_pr=unionOfLinkedandNotLinked_ol.groupBy("filename").agg(sum("isLinked") as "isLinked",sum("isNotLinked") as "isNotLinked",max("Source") as "Source")
agg_sum_no_linked_pr=agg_sum_no_linked_pr.withColumn("ENTITY",lit("ORDERLINES with PRODUCTS"))

// COMMAND ----------

agg_sum_no_linked_pr=agg_sum_no_linked_pr.withColumn("filename", split(agg_sum_no_linked_pr.col("filename"),"\\.")(0))
var orderlines_with_products=agg_sum_no_linked_pr.join(df,Seq("filename"),"left")

// COMMAND ----------

var incomingData_sb = spark
        .read
        .option("header", true)
        .option("sep", ";")
        .option("inferSchema", value = true)
        .csv("dbfs:/mnt/inbound/incoming/*/*SUBSCRIPTION*.csv")
        .select("CONTACT_PERSON_REF_ID","SOURCE_NAME")


var incomingData_cp_sb = spark
        .read
        .option("header", true)
        .option("sep", ";")
        .option("inferSchema", value = true)
        .csv("dbfs:/mnt/inbound/incoming/*/*CONTACTPERSONS*.csv")
        .select("REF_CONTACT_PERSON_ID","SOURCE")

// COMMAND ----------

var incomingData_sb_with_filename=incomingData_sb.withColumn("filename", split(input_file_name,"/")(5))

var incoming_sb=incomingData_sb.select("CONTACT_PERSON_REF_ID")
var incoming_cp_sb=incomingData_cp_sb.select("REF_CONTACT_PERSON_ID")

var with_link_cp_sb=incoming_sb.intersect(incoming_cp_sb)
with_link_cp_sb=with_link_cp_sb.dropDuplicates()
with_link_cp_sb=with_link_cp_sb.withColumn("isLinked",lit(1))
with_link_cp_sb=with_link_cp_sb.withColumn("isNotLinked",lit(0))
var with_link_cp_sb_final=with_link_cp_sb.join(incomingData_sb_with_filename,Seq("CONTACT_PERSON_REF_ID"),"inner")

var with_no_link_cp_sb=incoming_sb.except(incoming_cp_sb)
with_no_link_cp_sb = with_no_link_cp_sb.withColumn("isLinked",lit(0))
with_no_link_cp_sb=with_no_link_cp_sb.withColumn("isNotLinked",lit(1))
with_no_link_cp_sb=with_no_link_cp_sb.dropDuplicates()

var with_no_link_cp_sb_final=with_no_link_cp_sb.join(incomingData_sb_with_filename,Seq("CONTACT_PERSON_REF_ID"),"inner")

var unionOfLinkedandNotLinked_sb=with_link_cp_sb_final.unionByName(with_no_link_cp_sb_final)
var agg_sum_no_linked_sb=unionOfLinkedandNotLinked_sb.groupBy("filename").agg(sum("isLinked") as "isLinked",sum("isNotLinked") as "isNotLinked",max("SOURCE_NAME") as "Source")
agg_sum_no_linked_sb=agg_sum_no_linked_sb.withColumn("ENTITY",lit("Subscriptions with ContactPersons"))

// COMMAND ----------

agg_sum_no_linked_sb=agg_sum_no_linked_sb.withColumn("filename", split(agg_sum_no_linked_sb.col("filename"),"\\.")(0))
var subscriptions=agg_sum_no_linked_sb.join(df,Seq("filename"),"left")

// COMMAND ----------

var incomingData_lp = spark
        .read
        .option("header", true)
        .option("sep", ";")
        .option("inferSchema", value = true)
        .csv("dbfs:/mnt/inbound/incoming/*/*LOYALTY*.csv")
        .select("CONTACT_PERSON_REF_ID","SOURCE_NAME")


var incomingData_cp_lp = spark
        .read
        .option("header", true)
        .option("sep", ";")
        .option("inferSchema", value = true)
        .csv("dbfs:/mnt/inbound/incoming/*/*CONTACTPERSONS*.csv")
        .select("REF_CONTACT_PERSON_ID","SOURCE")

// COMMAND ----------

var incomingData_lp_with_filename=incomingData_lp.withColumn("filename", split(input_file_name,"/")(5))

var incoming_lp=incomingData_lp.select("CONTACT_PERSON_REF_ID")
var incoming_cp_lp=incomingData_cp_lp.select("REF_CONTACT_PERSON_ID")

var with_link_cp_lp=incoming_lp.intersect(incoming_cp_lp)
with_link_cp_lp=with_link_cp_lp.dropDuplicates()
with_link_cp_lp=with_link_cp_lp.withColumn("isLinked",lit(1))
with_link_cp_lp=with_link_cp_lp.withColumn("isNotLinked",lit(0))
var with_link_cp_lp_final=with_link_cp_lp.join(incomingData_lp_with_filename,Seq("CONTACT_PERSON_REF_ID"),"inner")

var with_no_link_cp_lp=incoming_lp.except(incoming_cp_lp)
with_no_link_cp_lp=with_no_link_cp_lp.withColumn("isLinked",lit(0))
with_no_link_cp_lp=with_no_link_cp_lp.withColumn("isNotLinked",lit(1))
with_no_link_cp_lp=with_no_link_cp_lp.dropDuplicates()

var with_no_link_cp_lp_final=with_no_link_cp_lp.join(incomingData_lp_with_filename,Seq("CONTACT_PERSON_REF_ID"),"inner")
var unionOfLinkedandNotLinked_lp=with_link_cp_lp_final.unionByName(with_no_link_cp_lp_final)
var agg_sum_no_linked_lp=unionOfLinkedandNotLinked_lp.groupBy("filename").agg(sum("isLinked") as "isLinked",sum("isNotLinked") as "isNotLinked",max("SOURCE_NAME") as "Source")
agg_sum_no_linked_lp=agg_sum_no_linked_lp.withColumn("ENTITY",lit("LoyaltyPoints with ContactPersons"))
agg_sum_no_linked_lp=agg_sum_no_linked_lp.withColumn("filename", split(agg_sum_no_linked_lp.col("filename"),"\\.")(0))

// COMMAND ----------

var loyalty_with_cp=agg_sum_no_linked_lp.join(df,Seq("filename"),"left")

// COMMAND ----------

var incomingData_lp_ = spark
        .read
        .option("header", true)
        .option("sep", ";")
        .option("inferSchema", value = true)
        .csv("dbfs:/mnt/inbound/incoming/*/*LOYALTY*.csv")
        .select("OPERATOR_REF_ID","SOURCE_NAME")


var incomingData_op_lp = spark
        .read
        .option("header", true)
        .option("sep", ";")
        .option("inferSchema", value = true)
        .csv("dbfs:/mnt/inbound/incoming/*/*OPERATORS*.csv")
        .select("REF_OPERATOR_ID","SOURCE")

// COMMAND ----------

var incomingData_lp_with_filename_ = incomingData_lp_.withColumn("filename", split(input_file_name,"/")(5))

var incoming_lp_ = incomingData_lp_.select("OPERATOR_REF_ID")
var incoming_op_lp=incomingData_op_lp.select("REF_OPERATOR_ID")

var with_link_op_lp=incoming_lp_.intersect(incoming_op_lp)
with_link_op_lp=with_link_op_lp.dropDuplicates()
with_link_op_lp=with_link_op_lp.withColumn("isLinked",lit(1))
with_link_op_lp=with_link_op_lp.withColumn("isNotLinked",lit(0))
var with_link_op_lp_final=with_link_op_lp.join(incomingData_lp_with_filename_,Seq("OPERATOR_REF_ID"),"inner")

var with_no_link_op_lp=incoming_lp_.except(incoming_op_lp)
with_no_link_op_lp=with_no_link_op_lp.withColumn("isLinked",lit(0))
with_no_link_op_lp=with_no_link_op_lp.withColumn("isNotLinked",lit(1))
with_no_link_op_lp=with_no_link_op_lp.dropDuplicates()

var with_no_link_op_lp_final=with_no_link_op_lp.join(incomingData_lp_with_filename_,Seq("OPERATOR_REF_ID"),"inner")
var unionOfLinkedandNotLinked_lp_op = with_link_op_lp_final.unionByName(with_no_link_op_lp_final)
var agg_sum_no_linked_lp_op=unionOfLinkedandNotLinked_lp_op.groupBy("filename").agg(sum("isLinked") as "isLinked",sum("isNotLinked") as "isNotLinked",max("SOURCE_NAME") as "Source")
agg_sum_no_linked_lp_op=agg_sum_no_linked_lp_op.withColumn("ENTITY",lit("LoyaltyPoints with Operators"))
agg_sum_no_linked_lp_op=agg_sum_no_linked_lp_op.withColumn("filename", split(agg_sum_no_linked_lp_op.col("filename"),"\\.")(0))

// COMMAND ----------

var lp_with_op=agg_sum_no_linked_lp_op.join(df,Seq("filename"),"left")

// COMMAND ----------

var incomingData_or_o = spark
        .read
        .option("header", true)
        .option("sep", ";")
        .option("inferSchema", value = true)
        .csv("dbfs:/mnt/inbound/incoming/*/*ORDERS*.csv")
        .select("REF_OPERATOR_ID","SOURCE")


var incomingData_op_or = spark
        .read
        .option("header", true)
        .option("sep", ";")
        .option("inferSchema", value = true)
        .csv("dbfs:/mnt/inbound/incoming/*/*OPERATORS*.csv")
        .select("REF_OPERATOR_ID","SOURCE")

// COMMAND ----------

var incomingData_or_o_with_filename = incomingData_or_o.withColumn("filename", split(input_file_name,"/")(5))

var incoming_or_o = incomingData_or_o.select("REF_OPERATOR_ID")
var incoming_op_or=incomingData_op_or.select("REF_OPERATOR_ID")

var with_link_op_or=incoming_or_o.intersect(incoming_op_or)
with_link_op_or=with_link_op_or.dropDuplicates()
with_link_op_or=with_link_op_or.withColumn("isLinked",lit(1))
with_link_op_or=with_link_op_or.withColumn("isNotLinked",lit(0))
var with_link_op_or_final=with_link_op_or.join(incomingData_or_o_with_filename,Seq("REF_OPERATOR_ID"),"inner")

var with_no_link_op_or=incoming_or_o.except(incoming_op_or)
with_no_link_op_or=with_no_link_op_or.withColumn("isLinked",lit(0))
with_no_link_op_or=with_no_link_op_or.withColumn("isNotLinked",lit(1))
with_no_link_op_or=with_no_link_op_or.dropDuplicates()

var with_no_link_op_or_final=with_no_link_op_or.join(incomingData_or_o_with_filename,Seq("REF_OPERATOR_ID"),"inner")
var unionOfLinkedandNotLinked_or_op = with_link_op_or_final.unionByName(with_no_link_op_or_final)
var agg_sum_no_linked_or_op=unionOfLinkedandNotLinked_or_op.groupBy("filename").agg(sum("isLinked") as "isLinked",sum("isNotLinked") as "isNotLinked",max("SOURCE") as "Source")
agg_sum_no_linked_or_op=agg_sum_no_linked_or_op.withColumn("ENTITY",lit("Orders with Operators"))
agg_sum_no_linked_or_op=agg_sum_no_linked_or_op.withColumn("filename", split(agg_sum_no_linked_or_op.col("filename"),"\\.")(0))

// COMMAND ----------

var or_with_op=agg_sum_no_linked_or_op.join(df,Seq("filename"),"left")

// COMMAND ----------

var incomingData_or_c = spark
        .read
        .option("header", true)
        .option("sep", ";")
        .option("inferSchema", value = true)
        .csv("dbfs:/mnt/inbound/incoming/*/*ORDERS*.csv")
        .select("REF_CONTACT_PERSON_ID","SOURCE")


var incomingData_cp_or = spark
        .read
        .option("header", true)
        .option("sep", ";")
        .option("inferSchema", value = true)
        .csv("dbfs:/mnt/inbound/incoming/*/*CONTACTPERSONS*.csv")
        .select("REF_CONTACT_PERSON_ID","SOURCE")

// COMMAND ----------

var incomingData_or_c_with_filename = incomingData_or_c.withColumn("filename", split(input_file_name,"/")(5))

var incoming_or_c = incomingData_or_c.select("REF_CONTACT_PERSON_ID")
var incoming_cp_or = incomingData_cp_or.select("REF_CONTACT_PERSON_ID")

var with_link_cp_or=incoming_or_c.intersect(incoming_cp_or)
with_link_cp_or=with_link_cp_or.dropDuplicates()
with_link_cp_or=with_link_cp_or.withColumn("isLinked",lit(1))
with_link_cp_or=with_link_cp_or.withColumn("isNotLinked",lit(0))
var with_link_cp_or_final=with_link_cp_or.join(incomingData_or_c_with_filename,Seq("REF_CONTACT_PERSON_ID"),"inner")

var with_no_link_cp_or=incoming_or_c.except(incoming_cp_or)
with_no_link_cp_or=with_no_link_cp_or.withColumn("isLinked",lit(0))
with_no_link_cp_or=with_no_link_cp_or.withColumn("isNotLinked",lit(1))
with_no_link_cp_or=with_no_link_cp_or.dropDuplicates()

var with_no_link_cp_or_final=with_no_link_cp_or.join(incomingData_or_c_with_filename,Seq("REF_CONTACT_PERSON_ID"),"inner")
var unionOfLinkedandNotLinked_or_cp = with_link_cp_or_final.unionByName(with_no_link_cp_or_final)
var agg_sum_no_linked_or_cp=unionOfLinkedandNotLinked_or_cp.groupBy("filename").agg(sum("isLinked") as "isLinked",sum("isNotLinked") as "isNotLinked",max("SOURCE") as "Source")
agg_sum_no_linked_or_cp=agg_sum_no_linked_or_cp.withColumn("ENTITY",lit("Orders with ContactPersons"))
agg_sum_no_linked_or_cp=agg_sum_no_linked_or_cp.withColumn("filename", split(agg_sum_no_linked_or_cp.col("filename"),"\\.")(0))

// COMMAND ----------

var or_with_cp=agg_sum_no_linked_or_cp.join(df,Seq("filename"),"left")

// COMMAND ----------

var incomingData_ac_o = spark
        .read
        .option("header", true)
        .option("sep", ";")
        .option("inferSchema", value = true)
        .csv("dbfs:/mnt/inbound/incoming/*/*OPERATOR_ACTIVITIES*.csv")
        .select("REF_OPERATOR_ID","SOURCE")


var incomingData_ac_op = spark
        .read
        .option("header", true)
        .option("sep", ";")
        .option("inferSchema", value = true)
        .csv("dbfs:/mnt/inbound/incoming/*/*OPERATORS*.csv")
        .select("REF_OPERATOR_ID","SOURCE")

// COMMAND ----------

var incomingData_ac_o_with_filename = incomingData_ac_o.withColumn("filename", split(input_file_name,"/")(5))

var incoming_ac_o = incomingData_ac_o.select("REF_OPERATOR_ID")
var incoming_ac_op = incomingData_ac_op.select("REF_OPERATOR_ID")

var with_link_ac_op=incoming_ac_o.intersect(incoming_ac_op)
with_link_ac_op=with_link_ac_op.dropDuplicates()
with_link_ac_op=with_link_ac_op.withColumn("isLinked",lit(1))
with_link_ac_op=with_link_ac_op.withColumn("isNotLinked",lit(0))
var with_link_ac_op_final=with_link_ac_op.join(incomingData_ac_o_with_filename,Seq("REF_OPERATOR_ID"),"inner")

var with_no_link_ac_op=incoming_ac_o.except(incoming_ac_op)
with_no_link_ac_op=with_no_link_ac_op.withColumn("isLinked",lit(0))
with_no_link_ac_op=with_no_link_ac_op.withColumn("isNotLinked",lit(1))
with_no_link_ac_op=with_no_link_ac_op.dropDuplicates()

var with_no_link_ac_op_final=with_no_link_ac_op.join(incomingData_ac_o_with_filename,Seq("REF_OPERATOR_ID"),"inner")
var unionOfLinkedandNotLinked_ac_op = with_link_ac_op_final.unionByName(with_no_link_ac_op_final)
var agg_sum_no_linked_ac_op=unionOfLinkedandNotLinked_ac_op.groupBy("filename").agg(sum("isLinked") as "isLinked",sum("isNotLinked") as "isNotLinked",max("SOURCE") as "Source")
agg_sum_no_linked_ac_op=agg_sum_no_linked_ac_op.withColumn("ENTITY",lit("Activities with Operators"))
agg_sum_no_linked_ac_op=agg_sum_no_linked_ac_op.withColumn("filename", split(agg_sum_no_linked_ac_op.col("filename"),"\\.")(0))

// COMMAND ----------

var activities_with_op=agg_sum_no_linked_ac_op.join(df,Seq("filename"),"left")

// COMMAND ----------

var incomingData_ac_c = spark
        .read
        .option("header", true)
        .option("sep", ";")
        .option("inferSchema", value = true)
        .csv("dbfs:/mnt/inbound/incoming/*/*CONTACTPERSON_ACTIVITIES_*.csv")
        .select("CONTACT_PERSON_REF_ID","SOURCE_NAME")


var incomingData_ac_cp = spark
        .read
        .option("header", true)
        .option("sep", ";")
        .option("inferSchema", value = true)
        .csv("dbfs:/mnt/inbound/incoming/*/*CONTACTPERSONS*.csv")
        .select("REF_CONTACT_PERSON_ID","SOURCE")

// COMMAND ----------

var incomingData_ac_c_with_filename = incomingData_ac_c.withColumn("filename", split(input_file_name,"/")(5))

var incoming_ac_c = incomingData_ac_c.select("CONTACT_PERSON_REF_ID")
var incoming_ac_cp = incomingData_ac_cp.select("REF_CONTACT_PERSON_ID")

var with_link_ac_cp=incoming_ac_c.intersect(incoming_ac_cp)
with_link_ac_cp=with_link_ac_cp.dropDuplicates()
with_link_ac_cp=with_link_ac_cp.withColumn("isLinked",lit(1))
with_link_ac_cp=with_link_ac_cp.withColumn("isNotLinked",lit(0))
var with_link_ac_cp_final=with_link_ac_cp.join(incomingData_ac_c_with_filename,Seq("CONTACT_PERSON_REF_ID"),"inner")

var with_no_link_ac_cp=incoming_ac_c.except(incoming_ac_cp)
with_no_link_ac_cp=with_no_link_ac_cp.withColumn("isLinked",lit(0))
with_no_link_ac_cp=with_no_link_ac_cp.withColumn("isNotLinked",lit(1))
with_no_link_ac_cp=with_no_link_ac_cp.dropDuplicates()

var with_no_link_ac_cp_final=with_no_link_ac_cp.join(incomingData_ac_c_with_filename,Seq("CONTACT_PERSON_REF_ID"),"inner")
var unionOfLinkedandNotLinked_ac_cp = with_link_ac_cp_final.unionByName(with_no_link_ac_cp_final)
var agg_sum_no_linked_ac_cp=unionOfLinkedandNotLinked_ac_cp.groupBy("filename").agg(sum("isLinked") as "isLinked",sum("isNotLinked") as "isNotLinked",max("SOURCE_NAME") as "Source")
agg_sum_no_linked_ac_cp=agg_sum_no_linked_ac_cp.withColumn("ENTITY",lit("Activities with ContactPersons"))
agg_sum_no_linked_ac_cp=agg_sum_no_linked_ac_cp.withColumn("filename", split(agg_sum_no_linked_ac_cp.col("filename"),"\\.")(0))

// COMMAND ----------

var activities_with_cp=agg_sum_no_linked_ac_cp.join(df,Seq("filename"),"left")

// COMMAND ----------

// MAGIC %md Campaigns start here

// COMMAND ----------

val data = Seq(
 ("FILE", "1")
,("WEBUPDATER", "2")
,("FUZZIT", "3")
,("DEX", "4")
,("BTG", "5")
,("MM-INIT-OPER", "6")
,("MM-INIT-NOOPER", "7")
,("EMAKINA", "8")
,("KANGAROO", "9")
,("ANTHEM", "10")
,("ARC", "11")
,("MELLOWMESSAGE", "12")
,("ACM", "13")
,("ARMSTRONG", "15")
,("BLITZ", "16")
,("BRANDTONE", "17")
,("RFB", "18")
,("WUFOO", "19")
,("TRACK", "20")
,("ASK_ROSANA", "21")
,("MOBILE", "22")
,("CRM_OTHER", "23")
,("COMPULSA", "24")
,("DATLINQ", "25")
,("EXTERNAL_FILES", "26")
,("GESCON", "27")
,("IOPERA", "28")
,("KLP", "29")
,("LOYALTY", "30")
,("PRIORITY_BAL", "31")
,("PRIORITY_CR", "32")
,("PRIORITY_SR", "33")
,("SRS", "34")
,("ONE_MOBILE", "35")
,("SFA_HISTORICAL_BAL", "36")
,("SFA_HISTORICAL_CR", "37")
,("SFA_HISTORICAL_SR", "38")
,("OTHER_BAL", "39")
,("OTHER_CR", "40")
,("OTHER_SR", "41")
,("TELESALES", "42")
,("CAMPAIGN_MARKETING", "43")
,("WEB_CRM", "44")
,("SSD_OTHER", "45")
,("SSD_MICROSITE", "46")
,("ADFORM", "47")
,("BBM", "48")
,("CALL_CENTER", "49")
,("E-COMMERCE_TRADE_PARTNER", "50")
,("FACEBOOK", "51")
,("FACEBOOK_MESSENGER", "52")
,("FOODSERVICE_DATABASE_CHD_EXPERT", "53")
,("GAZPACHO", "54")
,("GOOGLE_ADWORDS_SEARCH", "55")
,("GOOGLE_ANALYTICS", "56")
,("GOOGLE_DISPLAY_NETWORK", "57")
,("GOOGLE_PLACES", "58")
,("GOOGLE_SHOPPING", "59")
,("HORECA_SITES", "60")
,("INMOBI", "61")
,("INSTAGRAM", "62")
,("LINE", "63")
,("LINKEDIN", "64")
,("LIVECHAT", "65")
,("ODNOKLASSNIKI", "66")
,("OFFICE_SUPPLIERS", "67")
,("OFFICE_SUPPLIES_WEBSHOP_AKCAY", "68")
,("OFFICE_SUPPLIES_WEBSHOP_AVANSAS", "69")
,("OFFICE_SUPPLIES_WEBSHOP_BRUNEAU", "70")
,("OFFICE_SUPPLIES_WEBSHOP_BUNZL", "71")
,("OFFICE_SUPPLIES_WEBSHOP_CAPSUALE_CAFE", "72")
,("OFFICE_SUPPLIES_WEBSHOP_COS", "73")
,("OFFICE_SUPPLIES_WEBSHOP_JUSTE_A_TEMPS", "74")
,("OFFICE_SUPPLIES_WEBSHOP_LYRECO", "75")
,("OFFICE_SUPPLIES_WEBSHOP_OFFICE_DEPOT", "76")
,("OFFICE_SUPPLIES_WEBSHOP_OFFICE_DEPOT_VIKING", "77")
,("OFFICE_SUPPLIES_WEBSHOP_OFFICEMAX", "78")
,("OFFICE_SUPPLIES_WEBSHOP_PACKAGING_HOUSE", "79")
,("OFFICE_SUPPLIES_WEBSHOP_QUANTORE", "80")
,("OFFICE_SUPPLIES_WEBSHOP_QUILL", "81")
,("OFFICE_SUPPLIES_WEBSHOP_STAPLES", "82")
,("OFFICE_SUPPLIES_WEBSHOP_VOW", "83")
,("OTHER_SOURCE", "84")
,("PINTEREST", "85")
,("POCKETLINK", "86")
,("PROGRAMMATIC_AD_EXCHANGES", "87")
,("PURE_PLAYERS_ALACART", "88")
,("PURE_PLAYERS_ALIDI", "89")
,("PURE_PLAYERS_ALLEGRO", "90")
,("PURE_PLAYERS_AMAZON", "91")
,("PURE_PLAYERS_BLIBLI", "92")
,("PURE_PLAYERS_BOXED", "93")
,("PURE_PLAYERS_CATCH", "94")
,("PURE_PLAYERS_ELEVENIA", "95")
,("PURE_PLAYERS_FOODRAZOR", "96")
,("PURE_PLAYERS_FOODSERVICEDIRECT", "97")
,("PURE_PLAYERS_HEPSIBURADA", "98")
,("PURE_PLAYERS_INSTAMART", "99")
,("PURE_PLAYERS_JD", "100")
,("PURE_PLAYERS_JET", "101")
,("PURE_PLAYERS_LAZADA", "102")
,("PURE_PLAYERS_MARKET_MAN", "103")
,("PURE_PLAYERS_MATAHARI_MALL", "104")
,("PURE_PLAYERS_MEICAI", "105")
,("PURE_PLAYERS_MONOTARO", "106")
,("PURE_PLAYERS_SHOPEE", "107")
,("PURE_PLAYERS_TMALL", "108")
,("PURE_PLAYERS_WEBSTAURANTSTORE", "109")
,("PURE_PLAYERS_ZEEMART", "110")
,("SKYPE", "111")
,("TELEGRAM", "112")
,("TRADEPARTNER", "113")
,("TRADEPARTNER_WEBSHOP_AGM", "114")
,("TRADEPARTNER_WEBSHOP_ASKO", "115")
,("TRADEPARTNER_WEBSHOP_BIDVEST", "116")
,("TRADEPARTNER_WEBSHOP_BOOKER", "117")
,("TRADEPARTNER_WEBSHOP_BRAKES", "118")
,("TRADEPARTNER_WEBSHOP_CARL_EVENSEN", "119")
,("TRADEPARTNER_WEBSHOP_CATERING_ENGROS", "120")
,("TRADEPARTNER_WEBSHOP_CHECKERS_FS", "121")
,("TRADEPARTNER_WEBSHOP_CHEFS_CULINAR", "122")
,("TRADEPARTNER_WEBSHOP_COUNTRYWIDE", "123")
,("TRADEPARTNER_WEBSHOP_DANSK_CATER", "124")
,("TRADEPARTNER_WEBSHOP_EDEKA", "125")
,("TRADEPARTNER_WEBSHOP_FISHERMANS_DELI", "126")
,("TRADEPARTNER_WEBSHOP_GILMOURS", "127")
,("TRADEPARTNER_WEBSHOP_GOURMET_FOOD", "128")
,("TRADEPARTNER_WEBSHOP_GV_FOOD_UNION", "129")
,("TRADEPARTNER_WEBSHOP_HANOS", "130")
,("TRADEPARTNER_WEBSHOP_HORKRAM_FOODSERVICE", "131")
,("TRADEPARTNER_WEBSHOP_JAEGER", "132")
,("TRADEPARTNER_WEBSHOP_JAVA", "133")
,("TRADEPARTNER_WEBSHOP_KESPRO", "134")
,("TRADEPARTNER_WEBSHOP_LIMSIANGHUAT", "135")
,("TRADEPARTNER_WEBSHOP_LOTTE_ONLINE", "136")
,("TRADEPARTNER_WEBSHOP_MAIRA_NOVA", "137")
,("TRADEPARTNER_WEBSHOP_MAKRO", "138")
,("TRADEPARTNER_WEBSHOP_MARTIN_SERVERA", "139")
,("TRADEPARTNER_WEBSHOP_MENIGO", "140")
,("TRADEPARTNER_WEBSHOP_METRO", "141")
,("TRADEPARTNER_WEBSHOP_METRO_CC", "142")
,("TRADEPARTNER_WEBSHOP_MOCO", "143")
,("TRADEPARTNER_WEBSHOP_MUSGRAVES", "144")
,("TRADEPARTNER_WEBSHOP_PFD", "145")
,("TRADEPARTNER_WEBSHOP_PISTOR", "146")
,("TRADEPARTNER_WEBSHOP_REINHART", "147")
,("TRADEPARTNER_WEBSHOP_SCANA", "148")
,("TRADEPARTNER_WEBSHOP_SERVICEGROSSISTENE", "149")
,("TRADEPARTNER_WEBSHOP_SLIGRO", "150")
,("TRADEPARTNER_WEBSHOP_SVENSK_CATER", "151")
,("TRADEPARTNER_WEBSHOP_SYSCO", "152")
,("TRADEPARTNER_WEBSHOP_TEOSOONSENG", "153")
,("TRADEPARTNER_WEBSHOP_TOP_2_RESTOFRIT", "154")
,("TRADEPARTNER_WEBSHOP_TRANSGOURMET", "155")
,("TRADEPARTNER_WEBSHOP_TRENTS", "156")
,("TRADEPARTNER_WEBSHOP_TULUSMAJU", "157")
,("TRADEPARTNER_WEBSHOP_WEDL", "158")
,("TRADEPARTNER_WEBSHOP_WIHURI", "159")
,("TRADEPARTNER_WEBSHOP_ZORGBOODSCHAP", "160")
,("TRIP_ADVISOR", "161")
,("UCONTENT", "162")
,("UFS_APP", "163")
,("UFS_APP_CASH_AND_CARRY", "164")
,("UFS_APP_DSR", "165")
,("UFS_APP_SG", "166")
,("UFS_APP_UNICHEF", "167")
,("UFS_APP_WUOW", "168")
,("UFS_TRAINING_ACADEMY_APP", "169")
,("ULTRA_CELTRA", "170")
,("UNICHEF", "171")
,("USSD", "172")
,("VKONTAKTE", "173")
,("WECHAT", "174")
,("WHATSAPP", "175")
,("YELP", "176")
,("YOUTUBE", "177")
,("ZOMATO", "178")
,("FRONTIER", "179")
,("LEVEREDGE", "180")
,("CONSENT", "181")
,("I-SALES", "182")
,("OHUB1", "183")
,("PATCH", "184")
,("FAIR_KITCHENS", "185")
,("CATERLYST", "186")
,("FIREFLY", "187")
,("EXPORT", "188")
,("GOOGLE", "189")
,("UFS_ACADEMY_LITE", "190")
,("DATA_SCIENCE_CLOUD", "191")
,("GUVELLER", "192")
,("ARAWIN", "193")
,("FAIR", "194")
,("TRADE_ACTIVATIONS", "195")
,("DISTRIBUTOR_ACTIVATIONS", "196")
,("MEJORES_ARROCES", "197")
,("CLUB_HELLMANNS", "198")
,("MARKETO", "199")
,("OUNIVERSE", "200")
,("OUT_OF_HOME", "201")
,("MY_LOCAL_EATZ", "202")
,("PARDOT", "203"))

// COMMAND ----------

val mappingDf = data.toDF("SOURCE","SourceId")

// COMMAND ----------

var incomingData_cn = spark
        .read
        .option("header", true)
        .option("sep", ";")
        .option("inferSchema", value = true)
        .csv("dbfs:/mnt/inbound/incoming/*/*CAMPAIGNS*.csv")
        .select("Contact ID")


var incomingData_cp_cn = spark
        .read
        .option("header", true)
        .option("sep", ";")
        .option("inferSchema", value = true)
        .csv("dbfs:/mnt/inbound/incoming/*/*CONTACTPERSONS*.csv")
        .join(mappingDf,Seq("SOURCE"),"left")
        .withColumn("Contact ID",concat($"COUNTRY_CODE",lit("~"),$"REF_CONTACT_PERSON_ID",lit("~"),lit("3"),lit("~"),$"SourceId"))
        .select("Contact ID","Source")
        

// COMMAND ----------

var with_link_campaign=(incomingData_cn).join(incomingData_cp_cn,Seq("Contact ID"),"inner").dropDuplicates()
with_link_campaign=with_link_campaign.withColumn("isLinked",lit(1))
with_link_campaign=with_link_campaign.withColumn("isNotLinked",lit(0))

var with_no_link_campaign=incomingData_cn.except(incomingData_cp_cn.select("Contact ID")).dropDuplicates()
with_no_link_campaign=with_no_link_campaign.withColumn("isLinked",lit(0))
with_no_link_campaign=with_no_link_campaign.withColumn("isNotLinked",lit(1))

// COMMAND ----------

var incomingData_campaign_with_filename = incomingData_cn.withColumn("filename", split(input_file_name,"/")(5))

// COMMAND ----------

var campaign_cp=(with_link_campaign.select("Contact ID","isLinked","isNotLinked")).unionByName(with_no_link_campaign)
campaign_cp=(campaign_cp).join(with_link_campaign.select("Contact ID","Source"),Seq("Contact ID"),"left")

var with_cn_cp_final=campaign_cp.join(incomingData_campaign_with_filename,Seq("Contact ID"),"inner")

var agg_sum_linked_cn_cp=with_cn_cp_final.groupBy("filename").agg(sum("isLinked") as "isLinked",sum("isNotLinked") as "isNotLinked",max("SOURCE") as "Source")
agg_sum_linked_cn_cp=agg_sum_linked_cn_cp.withColumn("ENTITY",lit("Campaign with ContactPersons"))
agg_sum_linked_cn_cp=agg_sum_linked_cn_cp.withColumn("filename", split(agg_sum_linked_cn_cp.col("filename"),"\\.")(0))
agg_sum_linked_cn_cp=agg_sum_linked_cn_cp.join(df,Seq("filename"),"left")

// COMMAND ----------

var incomingData_cn_send = spark
        .read
        .option("header", true)
        .option("sep", ";")
        .option("inferSchema", value = true)
        .csv("dbfs:/mnt/inbound/incoming/*/*CAMPAIGN_SEND*.csv")
        .select("Contact ID")

// COMMAND ----------

var with_link_campaign_send=(incomingData_cn_send).join(incomingData_cp_cn,Seq("Contact ID"),"inner").dropDuplicates()
with_link_campaign_send=with_link_campaign_send.withColumn("isLinked",lit(1))
with_link_campaign_send=with_link_campaign_send.withColumn("isNotLinked",lit(0))

var with_no_link_campaign_send=incomingData_cn_send.except(incomingData_cp_cn.select("Contact ID")).dropDuplicates()
with_no_link_campaign_send=with_no_link_campaign_send.withColumn("isLinked",lit(0))
with_no_link_campaign_send=with_no_link_campaign_send.withColumn("isNotLinked",lit(1))

var incomingData_campaignsend_with_filename = incomingData_cn_send.withColumn("filename", split(input_file_name,"/")(5))

var campaign_send_cp=(with_link_campaign_send.select("Contact ID","isLinked","isNotLinked")).unionByName(with_no_link_campaign_send)
campaign_send_cp=(campaign_send_cp).join(with_link_campaign_send.select("Contact ID","Source"),Seq("Contact ID"),"left")

var with_cn_send_cp_final=campaign_send_cp.join(incomingData_campaignsend_with_filename,Seq("Contact ID"),"inner")

var agg_sum_linked_cn_send_cp=with_cn_send_cp_final.groupBy("filename").agg(sum("isLinked") as "isLinked",sum("isNotLinked") as "isNotLinked",max("SOURCE") as "Source")
agg_sum_linked_cn_send_cp=agg_sum_linked_cn_send_cp.withColumn("ENTITY",lit("Campaign Sends with ContactPersons"))
agg_sum_linked_cn_send_cp=agg_sum_linked_cn_send_cp.withColumn("filename", split(agg_sum_linked_cn_send_cp.col("filename"),"\\.")(0))
agg_sum_linked_cn_send_cp=agg_sum_linked_cn_send_cp.join(df,Seq("filename"),"left")

// COMMAND ----------

var incomingData_cn_click = spark
        .read
        .option("header", true)
        .option("sep", ";")
        .option("inferSchema", value = true)
        .csv("dbfs:/mnt/inbound/incoming/*/*CAMPAIGN_CLICK*.csv")
        .select("Contact ID")

// COMMAND ----------

var with_link_campaign_click=(incomingData_cn_click).join(incomingData_cp_cn,Seq("Contact ID"),"inner").dropDuplicates()
with_link_campaign_click=with_link_campaign_click.withColumn("isLinked",lit(1))
with_link_campaign_click=with_link_campaign_click.withColumn("isNotLinked",lit(0))

var with_no_link_campaign_click=incomingData_cn_click.except(incomingData_cp_cn.select("Contact ID")).dropDuplicates()
with_no_link_campaign_click=with_no_link_campaign_click.withColumn("isLinked",lit(0))
with_no_link_campaign_click=with_no_link_campaign_click.withColumn("isNotLinked",lit(1))

var incomingData_campaignclick_with_filename = incomingData_cn_click.withColumn("filename", split(input_file_name,"/")(5))

var campaign_click_cp=(with_link_campaign_click.select("Contact ID","isLinked","isNotLinked")).unionByName(with_no_link_campaign_click)
campaign_click_cp=(campaign_click_cp).join(with_link_campaign_click.select("Contact ID","Source"),Seq("Contact ID"),"left")

var with_cn_click_cp_final=campaign_click_cp.join(incomingData_campaignclick_with_filename,Seq("Contact ID"),"inner")

var agg_sum_linked_cn_click_cp=with_cn_click_cp_final.groupBy("filename").agg(sum("isLinked") as "isLinked",sum("isNotLinked") as "isNotLinked",max("SOURCE") as "Source")
agg_sum_linked_cn_click_cp=agg_sum_linked_cn_click_cp.withColumn("ENTITY",lit("Campaign clicks with ContactPersons"))
agg_sum_linked_cn_click_cp=agg_sum_linked_cn_click_cp.withColumn("filename", split(agg_sum_linked_cn_click_cp.col("filename"),"\\.")(0))
agg_sum_linked_cn_click_cp=agg_sum_linked_cn_click_cp.join(df,Seq("filename"),"left")

// COMMAND ----------

var incomingData_cn_open = spark
        .read
        .option("header", true)
        .option("sep", ";")
        .option("inferSchema", value = true)
        .csv("dbfs:/mnt/inbound/incoming/*/*CAMPAIGN_OPEN*.csv")
        .select("Contact ID")

// COMMAND ----------

var with_link_campaign_open=(incomingData_cn_open).join(incomingData_cp_cn,Seq("Contact ID"),"inner").dropDuplicates()
with_link_campaign_open=with_link_campaign_open.withColumn("isLinked",lit(1))
with_link_campaign_open=with_link_campaign_open.withColumn("isNotLinked",lit(0))

var with_no_link_campaign_open=incomingData_cn_open.except(incomingData_cp_cn.select("Contact ID")).dropDuplicates()
with_no_link_campaign_open=with_no_link_campaign_open.withColumn("isLinked",lit(0))
with_no_link_campaign_open=with_no_link_campaign_open.withColumn("isNotLinked",lit(1))

var incomingData_campaignopen_with_filename = incomingData_cn_open.withColumn("filename", split(input_file_name,"/")(5))

var campaign_open_cp=(with_link_campaign_open.select("Contact ID","isLinked","isNotLinked")).unionByName(with_no_link_campaign_open)
campaign_open_cp=(campaign_open_cp).join(with_link_campaign_open.select("Contact ID","Source"),Seq("Contact ID"),"left")

var with_cn_open_cp_final=campaign_open_cp.join(incomingData_campaignopen_with_filename,Seq("Contact ID"),"inner")

var agg_sum_linked_cn_open_cp=with_cn_open_cp_final.groupBy("filename").agg(sum("isLinked") as "isLinked",sum("isNotLinked") as "isNotLinked",max("SOURCE") as "Source")
agg_sum_linked_cn_open_cp=agg_sum_linked_cn_open_cp.withColumn("ENTITY",lit("Campaign opens with ContactPersons"))
agg_sum_linked_cn_open_cp=agg_sum_linked_cn_open_cp.withColumn("filename", split(agg_sum_linked_cn_open_cp.col("filename"),"\\.")(0))
agg_sum_linked_cn_open_cp=agg_sum_linked_cn_open_cp.join(df,Seq("filename"),"left")

// COMMAND ----------

var incomingData_cn_bounce = spark
        .read
        .option("header", true)
        .option("sep", ";")
        .option("inferSchema", value = true)
        .csv("dbfs:/mnt/inbound/incoming/*/*CAMPAIGN_BOUNCE*.csv")
        .select("Contact ID")

// COMMAND ----------

var with_link_campaign_bounce=(incomingData_cn_bounce).join(incomingData_cp_cn,Seq("Contact ID"),"inner").dropDuplicates()
with_link_campaign_bounce=with_link_campaign_bounce.withColumn("isLinked",lit(1))
with_link_campaign_bounce=with_link_campaign_bounce.withColumn("isNotLinked",lit(0))

var with_no_link_campaign_bounce=incomingData_cn_bounce.except(incomingData_cp_cn.select("Contact ID")).dropDuplicates()
with_no_link_campaign_bounce=with_no_link_campaign_bounce.withColumn("isLinked",lit(0))
with_no_link_campaign_bounce=with_no_link_campaign_bounce.withColumn("isNotLinked",lit(1))

var incomingData_campaignbounce_with_filename = incomingData_cn_bounce.withColumn("filename", split(input_file_name,"/")(5))

var campaign_bounce_cp=(with_link_campaign_bounce.select("Contact ID","isLinked","isNotLinked")).unionByName(with_no_link_campaign_bounce)
campaign_bounce_cp=(campaign_bounce_cp).join(with_link_campaign_bounce.select("Contact ID","Source"),Seq("Contact ID"),"left")

var with_cn_bounce_cp_final=campaign_bounce_cp.join(incomingData_campaignbounce_with_filename,Seq("Contact ID"),"inner")

var agg_sum_linked_cn_bounce_cp=with_cn_bounce_cp_final.groupBy("filename").agg(sum("isLinked") as "isLinked",sum("isNotLinked") as "isNotLinked",max("SOURCE") as "Source")
agg_sum_linked_cn_bounce_cp=agg_sum_linked_cn_bounce_cp.withColumn("ENTITY",lit("Campaign bounces with ContactPersons"))
agg_sum_linked_cn_bounce_cp=agg_sum_linked_cn_bounce_cp.withColumn("filename", split(agg_sum_linked_cn_bounce_cp.col("filename"),"\\.")(0))
agg_sum_linked_cn_bounce_cp=agg_sum_linked_cn_bounce_cp.join(df,Seq("filename"),"left")

// COMMAND ----------

var combined=contactpersons.unionByName(orderlines_with_orders).
unionByName(orderlines_with_products).
unionByName(subscriptions).
unionByName(loyalty_with_cp).
unionByName(lp_with_op).
unionByName(or_with_op).
unionByName(or_with_cp).
unionByName(activities_with_op).
unionByName(activities_with_cp).
unionByName(agg_sum_linked_cn_bounce_cp).
unionByName(agg_sum_linked_cn_click_cp).
unionByName(agg_sum_linked_cn_send_cp).
unionByName(agg_sum_linked_cn_cp)

// COMMAND ----------

// MAGIC %python
// MAGIC def file_exists(path):
// MAGIC   try:
// MAGIC     dbutils.fs.ls(path)
// MAGIC     return True
// MAGIC   except Exception as e:
// MAGIC     if 'java.io.FileNotFoundException' in str(e):
// MAGIC       return False
// MAGIC     else:
// MAGIC       raise
// MAGIC       
// MAGIC if(file_exists("dbfs:/mnt/inbound/insights/EntityLinkingInitialLoad/temp/EntityLinking_merged.csv")):
// MAGIC   dbutils.fs.rm("dbfs:/mnt/inbound/insights/EntityLinkingInitialLoad/temp/EntityLinking_merged.csv")

// COMMAND ----------

val insightsOutputPath = dbutils.widgets.get("insights_output_path")
writeToCsv(insightsOutputPath, "EntityLinkingInsights", combined)
