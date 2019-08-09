// Databricks notebook source
// Notebook to perform a full manual dump of all of ohub's records for a specific runId (see cmd 4)
// Export will only be performed when all domainEntities are finished, otherwise an Exception is thrown

import org.apache.spark.sql._

def writeCsv(
    dataframe: Dataset [_],
    location: String,
    fieldSeparator: String = ";",
    hasHeaders: Boolean = true,
    saveMode: SaveMode = SaveMode.Overwrite
  ) =
      dataframe
        .write
        .mode(saveMode)
        .option("header", hasHeaders)
        .option("sep", fieldSeparator)
        .option("encoding", "UTF-8")
        .option("escape", "\"")
        .csv(location)

def hasSuccessFile(path: String): Boolean = {
  try {
    val files = dbutils.fs.ls(path)
    return files.exists(_.name == "_SUCCESS")
  } catch {
    case _: Exception => false
  }
}

// COMMAND ----------

def exportDomain(runId: String, domain:String):Unit = {
  var row = spark.read.parquet(s"dbfs:/mnt/engine/integrated/${runId}/${domain}.parquet")
  row = row.drop("additionalFields", "ingestionErrors", "additives", "allergens", "dietetics", "nutrientTypes", "nutrientValues", "productCodes")
  writeCsv(row, s"dbfs:/mnt/datascience/${domain}/datascience")
}

// COMMAND ----------

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

// Get datestring for yesterday (== last run ID a.t.m.)
val runIdFormat = new SimpleDateFormat("yyyy-MM-dd")
val lastRunDate = Calendar.getInstance()
lastRunDate.add(Calendar.DATE, -1)
val runId = runIdFormat.format(lastRunDate.getTime())

// COMMAND ----------

val exportDomains = Seq(
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
                        "campaignopens",
                        "chains"
)

val allDomainsFinished = !exportDomains.map(domain => hasSuccessFile(s"dbfs:/mnt/engine/integrated/${runId}/${domain}.parquet")).contains(false)
if (allDomainsFinished) {
  exportDomains.foreach(exportDomain(runId, _))
  dbutils.notebook.exit(s"Exported all entities for runId ${runId}")
} else throw new RuntimeException("Not all pipeline runs have finished, so no export will be written. Run export manually when pipelines have finished.")
