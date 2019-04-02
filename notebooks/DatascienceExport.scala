// Databricks notebook source
// Notebook to perform a full manual dump of all of ohub's records for a specific runId (see cmd 3)

import org.apache.spark.sql._

def writeCsv(
    dataframe: Dataset [_],
    location: String,
    fieldSeparator: String = ";",
    hasHeaders: Boolean = true
  ) =
      dataframe
        .write
        .option("header", hasHeaders)
        .option("sep", fieldSeparator)
        .option("encoding", "UTF-8")
        .option("escape", "\"")
        .csv(location)

// COMMAND ----------

def exportDomain(runId: String, domain:String):Unit = {
  var row = spark.read.parquet(s"dbfs:/mnt/engine/integrated/${runId}/${domain}.parquet")
  row = row.drop("additionalFields", "ingestionErrors", "additives", "allergens", "dietetics", "nutrientTypes", "nutrientValues", "productCodes")
  writeCsv(row, s"dbfs:/mnt/outbound/datascience/${runId}/${domain}/datascience")
}

// COMMAND ----------

val runId = "2019-03-21"
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
                        "campaignopens"
)

exportDomains.foreach(exportDomain(runId, _))
