// Databricks notebook source
// MAGIC %md
// MAGIC ### Common methods

// COMMAND ----------

// MAGIC %run ./BaseInsightExporter

// COMMAND ----------

// MAGIC %md
// MAGIC ### SPECIFIC UDFs

// COMMAND ----------

val getSourceName = udf((fileName: String) => getModelAndSourceName(fileName)._1)
val getModelName  = udf((fileName: String) => getModelAndSourceName(fileName)._2)

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Logics

// COMMAND ----------

def getFileInsightData(execDate: String) = {
  
  lazy val insightQuery = "SELECT " +
          "DISTINCT ON(FU.file_name, split_part(FUE.error_message::TEXT,' ', 2))FU.file_name AS FileName, " +
          "BSE.read_count as TotalRecords, " +
          "COUNT(split_part(FUE.error_message::TEXT,' ', 2)) as ErrorTypeCountPerFile, " +
      //    "(BSE.read_count - COUNT(CASE WHEN FUE.is_warning)) as IngestedRecordsCount, " +
          "min(FUE.error_message) AS ErrorMessage, " +
          "CASE WHEN char_length(FU.user_name)-char_length(replace(FU.user_name,'-',''))=4 THEN spn.spnname ELSE FU.user_name END AS UserName, " +
          "CASE WHEN spnname like 'svc%' then  reverse(substring(reverse(spnname),1,strpos(reverse(spnname),'-')-1)) else COALESCE(ad.country,'NL') end as Country, " +
          "FU.status AS Status, " +
          "FU.timestamp as TimeStamp, " +
          "FU.reason_failed AS ReasonFailed, " +
          "FU.linked_file AS LinkedFile, " +
          "FUE.is_warning AS IsWarning " +
        "FROM inbound.AUDIT_TRAILS AS FU " +
        "LEFT JOIN inbound.errors AS FUE ON FU.file_name = FUE.file_name  " +
        "INNER JOIN inbound.batch_job_execution_params JEP ON JEP.string_val = FU.file_name  " +
        "INNER JOIN inbound.batch_step_execution BSE ON BSE.job_execution_id=JEP.job_execution_id " +
        "LEFT JOIN inbound.adgroupusers AS ad ON FU.user_name = ad.username " +
        "LEFT JOIN inbound.serviceprincipals as spn ON  spn.spnid = FU.user_name " +
          "WHERE (FU.status='COMPLETED' OR FU.status='FAILED') AND " +
          s"DATE(FU.timestamp) = '${execDate}' AND " +
          "BSE.step_name='model_step' " +
          "GROUP BY FU.file_name, split_part(FUE.error_message::TEXT,' ', 2), FUE.is_warning, BSE.read_count, JEP.string_val,ad.country, spn.spnname " +
          "ORDER BY FU.file_name,split_part(FUE.error_message::TEXT,' ', 2)"

    getProdDbData(insightQuery)
        .withColumnRenamed("filename", "FILE_NAME")
        .withColumnRenamed("totalrecords", "TOTAL_RECORDS")
        .withColumnRenamed("errortypecountperfile", "ERROR_TYPE_COUNT_PER_FILE")
    //    .withColumnRenamed("IngestedRecordsCount", "INGESTED_RECORDS_COUNT")
        .withColumnRenamed("errormessage", "ERROR_MESSAGE")
        .withColumnRenamed("username", "USER_NAME")
        .withColumnRenamed("status", "STATUS")
        .withColumnRenamed("country", "COUNTRY")
        .withColumnRenamed("timestamp", "TIMESTAMP")
        .withColumnRenamed("reasonfailed", "REASON_FAILED")
        .withColumnRenamed("linkedfile", "LINKED_FILE")
        .withColumnRenamed("iswarning", "IS_WARNING")
        .withColumn("SOURCE_NAME", getSourceName($"FILE_NAME"))
        .withColumn("ENTITY_NAME", getModelName($"FILE_NAME"))
        .withColumn("USER_NAME", when($"USER_NAME".contains("svc-b-da-"), expr("substring(USER_NAME, 18, length(USER_NAME))")) otherwise($"USER_NAME")) 
}

// COMMAND ----------

// MAGIC %md
// MAGIC ### GETTING INSIGHT DATA AND WRITING TO BLOB

// COMMAND ----------

def run(execDate: String, prevInsightsPath: String, insightsOutputPath: String) = {
  
  val deltaInsightsIntegratedDF = getFileInsightData(execDate).cache
  val prevInsightsIntegratedDF  = readCsvFile(prevInsightsPath, inferSchema = true).cache
  
  val integratedDF = prevInsightsIntegratedDF.unionByName(deltaInsightsIntegratedDF)
  println("Total deltaInsightsIntegratedDF :: " + deltaInsightsIntegratedDF.count())
  writeToCsv(insightsOutputPath, "FileUploadErrorsInsights", integratedDF)
}

// COMMAND ----------

// MAGIC %md
// MAGIC ### GETTING PARAMETERS FROM ADF

// COMMAND ----------

val execDate = dbutils.widgets.get("execDate")
val insightsOutputPath = dbutils.widgets.get("insightsOutputPath")
val prevInsightsPath = dbutils.widgets.get("prevInsightsPath")

run(execDate, prevInsightsPath, insightsOutputPath)


// COMMAND ----------

//display(readCsvFile("dbfs:/mnt/inbound/insights/2019-12-05/FileUploadErrorsInsight.csv").filter($"FILE_NAME" contains "ONE_MOB"))
