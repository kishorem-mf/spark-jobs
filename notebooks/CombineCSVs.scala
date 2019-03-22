// Databricks notebook source
// Notebook that combines multiple inbound exports into one

import org.apache.spark.sql.functions.{concat, lit, row_number, col}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql._
import org.apache.hadoop.fs._

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

// val febDays = (7 to 28).toList.map("02-" +"%02d".format(_))
val marchDays = (15 to 17).toList.map("03-" +"%02d".format(_))
// val days = febDays ++ marchDays
val days = marchDays

// COMMAND ----------

val domain = "loyaltypoints"
val allInbound = readCsv(s"dbfs:/mnt/inbound/${domain}/2019-{${days.mkString(",")}}/*.csv")

// COMMAND ----------

allInbound.count()

// COMMAND ----------

writeCsv(allInbound, s"dbfs:/mnt/inbound/${domain}/2019-03-17_combined")
