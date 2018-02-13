package com.unilever.ohub.spark.storage

import org.apache.spark.sql.{ Column, DataFrame, Dataset, Encoder, SaveMode, SparkSession }

import scala.io.Source

trait Storage {
  def readFromCSV(location: String, separator: String): DataFrame

  def writeToCSV(ds: Dataset[_], outputFile: String, partitionBy: String*): Unit

  def readFromParquet[T: Encoder](location: String, selectColumns: Column*): Dataset[T]

  def writeToParquet(ds: Dataset[_], location: String, partitionBy: String*): Unit

  def countries: Dataset[CountryRecord]

  def sourcePreference: Map[String, Int]
}

class DiskStorage(spark: SparkSession) extends Storage {
  import spark.implicits._

  override def readFromCSV(
    location: String,
    separator: String
  ): DataFrame = {
    spark
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("sep", separator)
      .csv(location)
  }

  def writeToCSV(ds: Dataset[_], outputFile: String, partitionBy: String*): Unit = {
    ds
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("encoding", "UTF-8")
      .option("header", "true")
      .option("delimiter","\u00B6")
      // This makes sure the when " is in a value it is not escaped like \" which is not accepted by ACM
      .option("quote", "\u0000")
      //.option("quoteAll", if (quoteAll) "true" else "false")
      .partitionBy(partitionBy: _*)
      .csv(outputFile)
  }

  override def readFromParquet[T: Encoder](location: String, selectColumns: Column*): Dataset[T] = {
    spark
      .read
      .parquet(location)
      .select(selectColumns: _*)
      .as[T]
  }

  override def writeToParquet(ds: Dataset[_], location: String, partitionBy: String*): Unit = {
    ds
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy(partitionBy: _*)
      .parquet(location)
  }

  override def countries: Dataset[CountryRecord] = {
    val fileName = this.getClass.getResource("country_codes.csv").getFile
    readFromCSV(fileName, separator = ",")
      .select(
        $"ISO3166_1_Alpha_2" as "COUNTRY_CODE",
        $"official_name_en" as "COUNTRY",
        $"ISO4217_currency_alphabetic_code" as "CURRENCY_CODE"
      )
      .where($"COUNTRY_CODE".isNotNull and $"COUNTRY".isNotNull and $"CURRENCY_CODE".isNotNull)
      .as[CountryRecord]
  }

  override def sourcePreference: Map[String, Int] = {
    Source
      .fromInputStream(this.getClass.getResourceAsStream("source_preference.tsv"))
      .getLines()
      .toSeq
      .filter(_.nonEmpty)
      .filterNot(_.equals("SOURCE\tPRIORITY"))
      .map(_.split("\t"))
      .map(lineParts => lineParts(0) -> lineParts(1).toInt)
      .toMap
  }
}
