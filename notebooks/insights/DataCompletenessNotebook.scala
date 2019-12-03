// Databricks notebook source
// MAGIC %md
// MAGIC ### Common methods initializing

// COMMAND ----------

// MAGIC %run ./BaseInsightExporter

// COMMAND ----------

// MAGIC %md
// MAGIC ### GET AUDIT TRAILS METHOD

// COMMAND ----------

def getAuditTrailsData() = {

 val query = "select " +
        "FU.file_name," +
        "FU.status, " +
        "FU.timestamp::date, " +
        "CASE WHEN char_length(FU.user_name)-char_length(replace(FU.user_name,'-',''))=4 THEN spn.spnname ELSE FU.user_name END AS UserName " +
      "FROM inbound.AUDIT_TRAILS AS FU " +
      "LEFT JOIN inbound.adgroupusers AS ad " +
        "ON FU.user_name = ad.username " +
      "LEFT JOIN inbound.serviceprincipals as spn " +
        "ON  spn.spnid = FU.user_name " +
      "where (FU.status='COMPLETED' OR FU.status='FAILED' OR FU.status='EXECUTING')  " +
        "GROUP BY FU.file_name, ad.country, spn.spnname"

  getProdDbData(query)
    .withColumn("file_name", upper($"file_name"))
    .withColumnRenamed("file_name", "AUDIT_FILE_NAME")
    .withColumnRenamed("timestamp", "CREATED_DATE")
    .withColumnRenamed("status", "STATUS")
    .withColumnRenamed("username", "USER_NAME")
    .withColumn("USER_NAME", when($"USER_NAME".contains("svc-b-da-"), expr("substring(USER_NAME, 18, length(USER_NAME))")) otherwise($"USER_NAME")) 
}

// COMMAND ----------

// MAGIC %md
// MAGIC ### GET LIST OF FILES IN BLOB 
// MAGIC Returns all the fileNames with Path present in the blob

// COMMAND ----------

def getFileListInBlobs(fromDate:String, toDate:String) = {
  getDatesInRange(fromDate, toDate)
    .map(date => s"dbfs:/mnt/inbound/incoming/${date}")
    .map(getAllFileInBlob(_))
    .flatten
    .filter(path => path.substring(38, path.size).matches("(^UFS_).*(\\.(?i)csv)"))
    .toList
}

// COMMAND ----------

// MAGIC %md
// MAGIC ### GETS THE INSIGHT INFORMATION FOR A FILE 
// MAGIC Takes filePath as argument and returns tuples

// COMMAND ----------

val contactPersonFpoFields = Seq("FIRST_NAME", "LAST_NAME", "ZIP_CODE", "STREET", "CITY", "MOBILE_PHONE_NUMBER", "EMAIL_ADDRESS", "EM_OPT_IN_CONFIRMED", "EM_OPT_IN_CONFIRMED_DATE", "EM_OPT_IN", "EM_OPT_IN_DATE", "EM_OPT_OUT", "MOB_OPT_IN_CONFIRMED", "MOB_OPT_IN_CONFIRMED_DATE", "MOB_OPT_IN", "MOB_OPT_IN_DATE", "MOB_OPT_OUT")

val operatorFpoFields = Seq("NAME", "ZIP_CODE", "STREET", "CITY", "CHANNEL")

def getFileInsights(incomingFilePath: String) = {
  
  try{
    val baseFileName = getBaseName(incomingFilePath)
    val incomingFileDF = readCsvFile(incomingFilePath).cache
    val totalRowCount = incomingFileDF.count

    val (modelName, sourceName) = getModelAndSourceName(baseFileName)

    val dataFilledPercentage = getDataFilledPercentage(incomingFileDF)

    val fpoFilledPercentage = modelName match {
      case "OPERATORS" => getDataFilledPercentage(incomingFileDF.select(operatorFpoFields.map(col): _*))
      case "CONTACTPERSONS" => getDataFilledPercentage(incomingFileDF.select(contactPersonFpoFields.map(col): _*))
      case _ => 0
    }  

    (baseFileName, modelName, sourceName, totalRowCount, dataFilledPercentage, fpoFilledPercentage)
    
  }catch {
    case e:  Exception => println(s"Issue in file :: ${incomingFilePath}")
    (incomingFilePath, "-", "-", 0l, 0d, 0d)
  }
}


// COMMAND ----------

// MAGIC %md
// MAGIC ### GET DELTA INTEGRATED INSIGHT INFO AND UNION IT WITH PREVIOUS INTEGRATED INSIGHT INFO 

// COMMAND ----------

def getDeltaIntegrated(execDate: String, prevExecDate: String) = {
  
  lazy val finalIncomingFilePaths = getFileListInBlobs(execDate, prevExecDate)

  val fileInsightDF = finalIncomingFilePaths
                      .par
                      .map(filePath => getFileInsights(filePath))
                      .seq
                      .toDF("FILE_NAME", "MODEL", "SOURCE", "TOTAL_ROW_COUNT", "DATA_FILLED_PERCENTAGE", "FPO_DATA_FILLED_PERCENTAGE")
                      .cache
  
  println(s"total files :: ${finalIncomingFilePaths.length}")
  
  val auditTrailDF = getAuditTrailsData().cache
  
  fileInsightDF
  .join(auditTrailDF, fileInsightDF("FILE_NAME") === auditTrailDF("AUDIT_FILE_NAME"), "left")
  .drop("AUDIT_FILE_NAME") 

}


def run(execDate: String, prevExecDate: String, prevInsightsPath: String, insightsOutputPath: String) = {
  
  val deltaIntegratedDF = getDeltaIntegrated(execDate, prevExecDate).cache
  val prevIntegratedDF  = readCsvFile(prevInsightsPath).cache
  
  val integratedDF = deltaIntegratedDF.unionByName(prevIntegratedDF)
  
  writeToBlob(insightsOutputPath, integratedDF)
}

// COMMAND ----------

// MAGIC %md
// MAGIC ### Initialize variables by getting values from ADF Pipeline
// MAGIC 
// MAGIC **prevExecDate** value will be equal to **execDate**. When we need to generate the insights for a date range then you can pass the **prevExecDate** from ADF pipeline

// COMMAND ----------

val execDate =dbutils.widgets.get("execDate")
val prevInsightsPath = dbutils.widgets.get("previousInsightsPath")
val insightsOutputPath = dbutils.widgets.get("insightsOutputPath")

dbutils.widgets.text("prevExecDate", "", "")
val prevExecDate = if(dbutils.widgets.get("prevExecDate") == "") execDate else dbutils.widgets.get("prevExecDate")

run(execDate, prevExecDate, prevInsightsPath, insightsOutputPath)



