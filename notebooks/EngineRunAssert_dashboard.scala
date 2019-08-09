// Databricks notebook source
// This notebook does counts and assertions for all entities in inbound, engine and outbound for the last engine-run (exec-date == today && files are all in exec-data - 1 day folders). 

// Util functions + global imports
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

spark.conf.set("spark.databricks.io.cache.enabled", "true")

def hasSuccessFile(path: String): Boolean = {
  try {
    val files = dbutils.fs.ls(path)
    return files.exists(_.name == "_SUCCESS")
  } catch {
    case _: Exception => false
  }
}

def readCsv(
    location: String,
    fieldSeparator: String = ";",
    hasHeaders: Boolean = true
  ): Dataset[Row] =
  try {
      spark
        .read
        .option("header", hasHeaders)
        .option("sep", fieldSeparator)
        .option("inferSchema", value = false)
        .option("encoding", "UTF-8")
        .option("escape", "\"")
        .csv(location)
  } catch {
    case _: Exception => spark.emptyDataFrame
  }

def readParquet(location: String): Dataset[Row] =
  try {
    spark
      .read
      .option("inferschema","true")
      .parquet(location)
  } catch {
    case _: Exception => spark.emptyDataFrame
  }

def tryCount(countFunc: () => Long) : Long = {
  try {
    countFunc()
  } catch {
    case _ : Exception => -1
  }
}

// COMMAND ----------

// Counts for seperate stages. If no _SUCCESS file is present in the corresponding dir, -1 wil be returned as count

def getInboundCounts(runId: String, domain: String) = {
  val inboundCsvDir = s"dbfs:/mnt/inbound/${domain}/${runId}"
  if(hasSuccessFile(inboundCsvDir)) {
    val inbound = readCsv(inboundCsvDir + "/*.csv")
    val inboundCount = tryCount(() => inbound.count())
    val inboundUniqueCount = tryCount(() => inbound.dropDuplicates("concatId").count())
    (inboundCount, inboundUniqueCount)
  } else (-1L, -1L)
}

def getIngestedCount(runId: String, domain: String) = {
  val ingestedDir = s"dbfs:/mnt/engine/ingested/${runId}/${domain}.parquet"
  if(hasSuccessFile(ingestedDir)) {
    val ingested = readParquet(ingestedDir)
    val ingestedCount = tryCount(() => ingested.count())
    val ingestionErrors = readParquet(s"dbfs:/mnt/engine/ingested/${runId}/${domain}.parquet.errors")
    val ingestionErrorsCount = tryCount(() => ingestionErrors.count())
    (ingestedCount, ingestionErrorsCount)  
  } else (-1L, -1L)  
}

def getIntegratedCount(runId: String, domain: String) = {
  val integratedDir = s"dbfs:/mnt/engine/integrated/${runId}/${domain}.parquet"
  if(hasSuccessFile(integratedDir)) {
    val integrated = readParquet(integratedDir)
    tryCount(() => integrated.count())
  } else -1L
}

def getChangedRows(domain: String, runId: String) = {
  val hash = readParquet(s"dbfs:/mnt/engine/hash/${runId}/${domain}.parquet")
  val rows = readParquet(s"dbfs:/mnt/engine/integrated/${runId}/${domain}.parquet")
  try {
    rows.join(hash, rows("concatId") === hash("concatId"), "inner").filter("hasChanged").drop(hash.columns:_*)
  } catch {
    case _ : Exception => spark.emptyDataFrame
  }
}

def getChangedCount(runId: String, domain: String) = {
  val hashDir = s"dbfs:/mnt/engine/hash/${runId}/${domain}.parquet"
  if (hasSuccessFile(hashDir)) {
    val changed = getChangedRows(domain, runId)
    val changedCount = tryCount(() => changed.count())
    val customChangedCount = domain match {
      case "orders" => tryCount(() => changed.filter(!$"type".isin("SSD", "TRANSFER")).count())
      case "orderlines" => tryCount(() => changed.filter($"orderType".isNull || !$"orderType".isin("SSD", "TRANSFER")).count())
      case "activities" => tryCount(() => changed.filter($"customerType" === "CONTACTPERSON").count())
      case _ => 0L
    }
    val changedGoldenCount = tryCount(() => changed.filter($"isGoldenRecord").count())
    val customChangedGoldenCount = domain match {
      case "orders" => tryCount(() => changed.filter(!$"type".isin("SSD", "TRANSFER") && $"isGoldenRecord").count())
      case "orderlines" => tryCount(() => changed.filter(($"orderType".isNull || !$"orderType".isin("SSD", "TRANSFER")) && $"isGoldenRecord").count())
      case "activities" => tryCount(() => changed.filter($"customerType" === "CONTACTPERSON" && $"isGoldenRecord").count())
      case _ => 0L
    }
    (changedCount, customChangedCount, changedGoldenCount, customChangedGoldenCount)
  } else (-1L, -1L, -1L, -1L)
}

def getOutboundCount(runId: String, domain: String) = {
  // Outbound doesn't produce a _SUCCESS file
  val outboundDispatch = readCsv(s"dbfs:/mnt/outbound/${domain}/${runId}/UFS_DISPATCH*.csv") // Dispatcher uses pilcrow as seperator(not ';'), so only count will work fine a.t.m.
  val outboundDispatchCount = tryCount(() => outboundDispatch.count())
  val acmDomain = domain match { 
    // ACM uses different filenames for some entities
    case "contactpersons" => "recipient"
    case "loyaltypoints" => "loyalties"
    case _ => domain
  }
  val outboundAcm = readCsv(s"dbfs:/mnt/outbound/${domain}/${runId}/UFS_${acmDomain.toUpperCase()}*.csv")
  val outboundAcmCount = tryCount(() => outboundAcm.count())
  (outboundDispatchCount, outboundAcmCount)
}

// COMMAND ----------

// Gather all counts per entity
def getCountsForDomain(runId: String, domain: String) = {
  val (inboundCount, inboundUniqueCount) = getInboundCounts(runId: String, domain: String)
  
  val (ingestedCount, ingestionErrorsCount) = getIngestedCount(runId: String, domain: String)
    
  val integratedCount = getIntegratedCount(runId: String, domain: String)
  
  val (changedCount, orderChangedCount, changedGoldenCount, orderChangedGoldenCount) = getChangedCount(runId: String, domain: String)
  
  val (outboundDispatchCount, outboundAcmCount) = getOutboundCount(runId: String, domain: String)
  
  (domain,
   inboundCount,
   inboundUniqueCount,
   ingestedCount,
   ingestionErrorsCount,
   integratedCount,
   changedCount,
   orderChangedCount,
   changedGoldenCount, 
   orderChangedGoldenCount,
   outboundDispatchCount,
   outboundAcmCount
  )  
}

// Get counts for all entities
def getCounts(runId: String, domains: String*) = {
  domains.map(domain => {
    getCountsForDomain(runId: String, domain: String)
  }).toDF(
    "domain",
    "inboundCount",
    "inboundUniqueCount",
    "ingestedCount",
    "ingestionErrorsCount",
    "integratedCount",
    "changedCount",
    "customChangedCount",
    "changedGoldenCount",
    "customChangedGoldenCount",
    "outboundDispatchCount",
    "outboundAcmCount"
  )
}

// COMMAND ----------

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

// Get datestring for yesterday (== last run ID a.t.m.)
val runIdFormat = new SimpleDateFormat("yyyy-MM-dd")
val lastRunDate = Calendar.getInstance()
lastRunDate.add(Calendar.DATE, -1)
val runId = runIdFormat.format(lastRunDate.getTime())

// All entities that are counted
val allDomains = Seq(
  "operators",
  "contactpersons",
  "orders",
  "orderlines",
  "loyaltypoints",
  "subscriptions", 
  "activities",
  "questions",
  "answers",
  "products",
  "campaigns",
  "campaignclicks",
  "campaignbounces",
  "campaignsends",
  "campaignopens"
)

// COMMAND ----------

val counts = getCounts(runId, allDomains :_*)

// COMMAND ----------

val exportedToAcm = Seq(
  "operators", 
  "contactpersons", 
  "orders", 
  "ordelines", 
  "loyaltypoints", 
  "subsciptions", 
  "activities", 
  "products"
)

val exportedToDispatch = Seq(
  "operators",
  "contactpersons",
  "orders",
  "orderlines",
  "loyaltypoints",
  "subscriptions", 
  "activities",
  "products",
  "campaignclicks",
  "campaignbounces",
  "campaigns",
  "campaignsends",
  "campaignopens"
)

// COMMAND ----------

// Generic asserts
val asserts = counts
  .withColumn("assertPipelineFinished", $"integratedCount" =!= -1) // This assertion does not include the content of the DB due to performance (but DB is implicitly covered in outbound counts)
  .withColumn("assertAllIngested", $"inboundUniqueCount" === $"ingestedCount")
  .withColumn("assertNoIngestionsErrors", $"ingestionErrorsCount" === 0)

// Asserts for orders and orderlines
val customAsserts = asserts
  .filter($"domain".isin("activities", "orders", "orderlines"))
  .withColumn("assertDispatchExportComplete", $"assertPipelineFinished" && $"customChangedCount" === $"outboundDispatchCount")
  .withColumn("assertAcmExportComplete", $"assertPipelineFinished" && $"customChangedGoldenCount" === $"outboundAcmCount")

val allCountsAsserts = asserts
  .filter(!$"domain".isin("orders", "orderlines"))
  .withColumn("assertDispatchExportComplete", $"assertPipelineFinished" && (!$"domain".isin(exportedToDispatch:_*)) || ($"changedCount" === $"outboundDispatchCount"))
  .withColumn("assertAcmExportComplete", $"assertPipelineFinished" && (!$"domain".isin(exportedToAcm:_*)) || ($"changedGoldenCount" === $"outboundAcmCount"))
  .union(customAsserts)

val allAsserts = allCountsAsserts.select("domain", allCountsAsserts.columns.filter(_.startsWith("assert")):_*)
val assertResult = allAsserts
  .toDF(allAsserts.columns.map((col) => col.replace("assert", "")) :_*)
  .orderBy($"domain")

// COMMAND ----------

assertResult.coalesce(1).write.mode(SaveMode.Overwrite).json(s"dbfs:/mnt/inbound/runresult/${runId}/assert")
counts.coalesce(1).write.mode(SaveMode.Overwrite).json(s"dbfs:/mnt/inbound/runresult/${runId}/count")

// COMMAND ----------

displayHTML(s"<h1>Run for data provided on ${runId}</h1>")

// COMMAND ----------

val allDone = assertResult.withColumn("done", $"PipelineFinished" && $"AllIngested" && $"NoIngestionsErrors" && $"DispatchExportComplete" && $"AcmExportComplete").filter(!$"done").count() == 0
allDone match {
  case true => displayHTML("<p style='color:green;''>All pipelines have run successfully</p>")
  case false => displayHTML("<p style='color:red'>All pipelines have <strong>NOT</strong> run successfully</p>")
}

// COMMAND ----------

// DBTITLE 1,Assertions
display(assertResult)

// COMMAND ----------

// DBTITLE 1,Counts
display(counts.orderBy($"domain".asc))
