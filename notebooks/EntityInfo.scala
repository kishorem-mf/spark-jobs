// Databricks notebook source
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import java.sql.Timestamp

// COMMAND ----------

def hasSuccessFile(path: String): Boolean = {
  try {
    val files = dbutils.fs.ls(path)
    return files.exists(_.name == "_SUCCESS")
  } catch {
    case _: Exception => false
  }
}

// COMMAND ----------

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
).sorted

val retentionDays = 5 to 200 by 5 map(_.toString)

// COMMAND ----------

dbutils.widgets.removeAll()
val domainDropdown = dbutils.widgets.dropdown("DomainPick", "operators", allDomains, "Entity")
val retentionDropdown = dbutils.widgets.dropdown("RetentionPick", "10", retentionDays, "Retention [days]")

// COMMAND ----------

def getIntegratedLoc()(implicit locationInfo: (String,String)) = {val (domain, runId) = locationInfo; s"dbfs:/mnt/engine/integrated/${runId}/${domain}.parquet"}

// COMMAND ----------

val domain = dbutils.widgets.get("DomainPick")

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

// Get datestring for yesterday (== last run ID a.t.m.)
val runIdFormat = new SimpleDateFormat("yyyy-MM-dd")

val lastRunDate = Calendar.getInstance()

def getLatestSuccesfullRunDate(): String = {
  lastRunDate.add(Calendar.DATE, -1)
  val runId = runIdFormat.format(lastRunDate.getTime())
  implicit val locationInfo = (domain, runId)
  hasSuccessFile(getIntegratedLoc()) match {
    case true => runId
    case false => getLatestSuccesfullRunDate()
  }
}

val runId = getLatestSuccesfullRunDate()

implicit val locationInfo = (domain, runId)
val integrated = spark.read.parquet(getIntegratedLoc())

// COMMAND ----------

val retention = dbutils.widgets.get("RetentionPick").toInt

val lowerBoundCal = Calendar.getInstance()
lowerBoundCal.add(Calendar.DATE, -1 * retention)

val lowerBound = new Timestamp(lowerBoundCal.getTimeInMillis())
val df = spark.read.parquet(s"dbfs:/mnt/engine/integrated/${runId}/${domain}.parquet")
val ingestedPerDay = df.withColumn("ingestDay", date_trunc("DAY", $"ohubUpdated"))
  .groupBy("ingestDay", "sourceName")
  .count()
  .filter($"ingestDay" > lowerBound)  
  .orderBy($"ingestDay".asc, $"count".desc)  

display(ingestedPerDay)

// COMMAND ----------


val aggHistWindow = Window.partitionBy($"ingestDay")
// val lowerBound = new Timestamp(0)
val ingestedPerDay = integrated
  .withColumn("ingestDay", date_trunc("DAY", $"ohubUpdated"))
  .groupBy($"ingestDay").count()
//   .withColumn("firstDay", lit(to_date(lit("2000-01-01"), "yyyy-MM-dd")))
//   .withColumn("totalAmmountOfRecords", sum($"count").over(aggHistWindow.rangeBetween(lit(to_date(lit("2000-01-01"), "yyyy-MM-dd")), $"ingestDay")))
  .filter($"ingestDay" > lowerBound)  
  .orderBy($"ingestDay".asc, $"count".desc)

// COMMAND ----------

val sourceCount = integrated.groupBy($"sourceName").count().orderBy($"count".desc)
display(sourceCount)

// COMMAND ----------

val goldenCount = integrated.groupBy($"isGoldenRecord").count().orderBy($"count".desc)
display(goldenCount)

// COMMAND ----------

val countryCount = integrated.groupBy($"countryCode").count().orderBy($"count".desc)
display(countryCount)
