package com.unilever.ohub.spark.storage

import org.apache.spark.sql.{ Column, Dataset, Encoder, SaveMode, SparkSession }

import scala.io.Source

trait Storage {
  def readFromCSV[T: Encoder](location: String, delimiter: String = ","): Dataset[T]

  def readFromParquet[T: Encoder](location: String, selectColumns: Column*): Dataset[T]

  def writeToParquet(ds: Dataset[_], location: String, partitionBy: String*): Unit

  def countries: Dataset[CountryRecord]

  def sourcePreference: Map[String, Int]
}

class DiskStorage(spark: SparkSession) extends Storage {
  import spark.implicits._

  override def readFromCSV[T: Encoder](location: String, delimiter: String = ","): Dataset[T] = {
    spark
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", delimiter)
      .csv(location)
      .as[T]
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
    readFromCSV[CountryRecord]("/country_codes.csv")
  }

  override def sourcePreference: Map[String, Int] = Source
    .fromInputStream(this.getClass.getResourceAsStream("/source_preference.tsv"))
    .getLines()
    .toSeq
    .filter(_.nonEmpty)
    .filterNot(_.equals("SOURCE\tPRIORITY"))
    .map(_.split("\t"))
    .map(lineParts => lineParts(0) -> lineParts(1).toInt)
    .toMap
}
