// Databricks notebook source
// MAGIC %md
// MAGIC READ WRITE FILE METHODS

// COMMAND ----------

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window   

import spark.implicits._ 

def readCsvFile(path:String, separator:String = ";") : DataFrame = {
  spark
        .read
        .option("header", true)
        .option("sep", separator)
        .option("inferSchema", value = false)
        .csv(path)        
}


def getParquetFile(domain:String, runId: String) = spark.read.parquet(s"dbfs:/mnt/engine/integrated/${runId}/${domain}.parquet")

def writeToBlob(path:String, resultantDF:DataFrame) = {
  resultantDF
   .coalesce(1)
   .write.format("com.databricks.spark.csv")
   .option("header", true)
   .option("delimiter", ";")
   .option("quoteAll", true)
  .save(path)
}

// COMMAND ----------

// MAGIC %md
// MAGIC GET COUNT METHODS

// COMMAND ----------


def tryCount(countFunc: () => Long) : Long = {
  try {
    countFunc()
  } catch {
    case _ : Exception => -1
  }
}

// COMMAND ----------

// MAGIC %md
// MAGIC GET MODEL AND SOURCE NAME METHOD

// COMMAND ----------

val CSV_NAME_PATTERN = """UFS_([A-Z0-9_-]+)_(CHAINS|LOYALTY|OPERATORS|OPERATOR_ACTIVITIES|OPERATOR_CLASSIFICATIONS|CONTACTPERSONS|CONTACTPERSON_ACTIVITIES|CONTACTPERSON_CLASSIFICATIONS|CONTACTPERSON_SOCIAL|CONTACTPERSON_ANONYMIZATIONS|PRODUCTS|ORDERS|ORDERLINES|ORDERS_DELETED|ORDERLINES_DELETED|SUBSCRIPTIONS|QUESTIONS|ANSWERS|CAMPAIGN_OPENS|CAMPAIGN_CLICKS|CAMPAIGN_BOUNCES|CAMPAIGNS|CAMPAIGN_SENDS|CHANNEL_MAPPINGS)_([0-9]{14})""".r

def getModelAndSourceName(csvFileName: String) =  {
 csvFileName.toUpperCase() match {
    case CSV_NAME_PATTERN(sourceName,modelName,_) => (sourceName, modelName)
    case _ =>("-","-")
   }
}

// COMMAND ----------

// MAGIC %md
// MAGIC QUERY PROD DB METHODS

// COMMAND ----------

def getProdDbData(query: String) = {

  val driver = "org.postgresql.Driver"
  val dbUrl   = dbutils.secrets.get(scope="ohub2-key-vault-secrets", key="dbUrl")
  val dbUser  = dbutils.secrets.get(scope="ohub2-key-vault-secrets", key="dbUserName")
  val dbPassword = dbutils.secrets.get(scope="ohub2-key-vault-secrets", key="dbPassword")

  spark.read.format("jdbc")
    .option("driver", driver)
    .option("url", dbUrl)
    .option("query", query)
    .option("user", dbUser)
    .option("password", dbPassword)
    .load()
}

// COMMAND ----------

// MAGIC %md
// MAGIC BLOB UTILITIES METHODS

// COMMAND ----------

import org.joda.time.format.DateTimeFormat
import org.joda.time.{LocalDate, Period}

def getDatesInRange(fromDate: String, toDate: String) = {

  def dateRange(from: LocalDate, to: LocalDate, step: Period): Iterator[LocalDate] =
    Iterator.iterate(from)(_.plus(step)).takeWhile(!_.isAfter(to))

  val dateFormat = DateTimeFormat.forPattern("yyyy-MM-dd")

  val fromDateLoc = LocalDate.parse(fromDate, dateFormat)
  val toDateLoc = LocalDate.parse(toDate, dateFormat)

  dateRange(fromDateLoc, toDateLoc, new Period().withDays(1)).map(_.toString)
}


def getAllFileInBlob (sourcePath:String) = {
  try {
    dbutils.fs.ls(sourcePath).map(fileInfo => fileInfo.path) 
  } 
  catch {
    case e : Exception => println(s"-------No files in ${sourcePath}")
    Seq()
  }
}

def roundOffValue(input: Double) = {
  Math.round(input * 100.0) / 100.0
}

val getBaseName = (path: String) => path.substring(path.lastIndexOf("UFS_"), path.lastIndexOf(".")).toUpperCase


// COMMAND ----------

// MAGIC %md
// MAGIC DATA FILLED PERCENTAGE METHODS

// COMMAND ----------

def getDataFilledPercentage(incomingDF: DataFrame) = {
  
  val columnCount = incomingDF.columns.filterNot(_.startsWith("_c")).size
  val totalRowCount = incomingDF.count
  val totalCellCount = columnCount * totalRowCount
  
  val notNullCountDF = incomingDF.describe().filter($"summary" === "count").drop("summary")
  val columnList: List[Column] = notNullCountDF.columns.map(col).toList
  val sum = notNullCountDF.withColumn("countTotal", columnList.reduce(_+_))
  val filledCount = sum.select("countTotal").as[Double].collect()

  val percentage = (filledCount(0)/totalCellCount*100)
  roundOffValue(percentage)
}
