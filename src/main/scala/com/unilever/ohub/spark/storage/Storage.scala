package com.unilever.ohub.spark.storage

import java.util.Properties

import com.unilever.ohub.spark.data.{ ChannelMapping, CountryRecord }
import com.unilever.ohub.spark.sql.JoinType
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{ Column, DataFrame, Dataset, Encoder, Row, SaveMode, SparkSession }
import java.io.{ File, FileOutputStream }

import scala.io.Source

trait Storage {
  def readFromCsv(
    location: String,
    fieldSeparator: String,
    hasHeaders: Boolean = true
  ): Dataset[Row]

  def writeToCsv(
    ds: Dataset[_],
    outputFile: String,
    partitionBy: Seq[String] = Seq(),
    delim: String = ";",
    quote: String = "\""
  ): Unit

  def readFromParquet[T: Encoder](location: String, selectColumns: Seq[Column] = Seq()): Dataset[T]

  def writeToParquet(ds: Dataset[_], location: String, partitionBy: Seq[String] = Seq()): Unit

  def createCountries: Dataset[CountryRecord]

  def sourcePreference: Map[String, Int]

  def channelMappings(
    dbUrl: String,
    dbName: String,
    userName: String,
    userPassword: String): Dataset[ChannelMapping]
}

class DefaultStorage(spark: SparkSession) extends Storage {
  import spark.implicits._

  override def readFromCsv(
    location: String,
    fieldSeparator: String,
    hasHeaders: Boolean = true
  ): Dataset[Row] = {
    spark
      .read
      .option("header", hasHeaders)
      .option("sep", fieldSeparator)
      .option("inferSchema", value = false)
      //      .option("mode", "FAILFAST") // let's fail fast for now
      .csv(location)
  }

  def writeToCsv(
    ds: Dataset[_],
    outputFile: String,
    partitionBy: Seq[String] = Seq(),
    delim: String = ";",
    quote: String = "\""
  ): Unit = {
    ds
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("encoding", "UTF-8")
      .option("header", "true")
      .option("quoteAll", "true")
      .option("delimiter", delim)
      .option("quote", quote)
      .partitionBy(partitionBy: _*)
      .csv(outputFile)
  }

  override def readFromParquet[T: Encoder](location: String, selectColumns: Seq[Column] = Seq()): Dataset[T] = {
    val parquetDF = spark
      .read
      .parquet(location)

    val parquetSelectDF = {
      if (selectColumns.nonEmpty) parquetDF.select(selectColumns: _*)
      else parquetDF
    }

    parquetSelectDF.as[T]
  }

  override def writeToParquet(ds: Dataset[_], location: String, partitionBy: Seq[String] = Seq()): Unit = {
    ds
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy(partitionBy: _*)
      .parquet(location)
  }

  override val createCountries: Dataset[CountryRecord] = {
    val file = "country_codes.csv"
    val in = this.getClass.getResourceAsStream(s"/$file")
    val out = new FileOutputStream(new File(file))

    Iterator
      .continually(in.read)
      .takeWhile(_ != -1)
      .foreach(b ⇒ out.write(b))

    out.close()
    in.close()

    readFromCsv(file, fieldSeparator = ",")
      .select(
        $"ISO3166_1_Alpha_2" as "countryCode",
        $"official_name_en" as "countryName",
        $"ISO4217_currency_alphabetic_code" as "currencyCode"
      )
      .where($"countryCode".isNotNull and $"countryName".isNotNull and $"currencyCode".isNotNull)
      .as[CountryRecord]
  }

  override def sourcePreference: Map[String, Int] = {
    Source
      .fromInputStream(this.getClass.getResourceAsStream("/source_preference.tsv"))
      .getLines()
      .toSeq
      .filter(_.nonEmpty)
      .filterNot(_.equals("SOURCE\tPRIORITY"))
      .map(_.split("\t"))
      .map(lineParts ⇒ lineParts(0) -> lineParts(1).toInt)
      .toMap
  }

  private def readJdbcTable(
    spark: SparkSession,
    dbUrl: String,
    dbName: String,
    dbTable: String,
    userName: String,
    userPassword: String
  ): DataFrame = {
    val dbFullConnectionString = s"jdbc::postgresql://$dbUrl:5432/$dbName"

    val jdbcProperties = new Properties
    jdbcProperties.put("user", userName)
    jdbcProperties.put("password", userPassword)

    spark.read.jdbc(dbFullConnectionString, dbTable, jdbcProperties)
  }

  override def channelMappings(
    dbUrl: String,
    dbName: String,
    userName: String,
    userPassword: String): Dataset[ChannelMapping] = {

    val channelMappingDF = readJdbcTable(spark, dbUrl, dbName, "channel_mapping", userName, userPassword)
    val channelReferencesDF = readJdbcTable(spark, dbUrl, dbName, "channel_references", userName, userPassword)
    channelMappingDF
      .join(
        channelReferencesDF,
        col("channel_reference_fk") === col("channel_reference_id"),
        JoinType.Left
      )
      .select(
        $"COUNTRY_CODE" as "countryCode",
        $"ORIGINAL_CHANNEL" as "originalChannel",
        $"LOCAL_CHANNEL" as "localChannel",
        $"CHANNEL_USAGE" as "channelUsage",
        $"SOCIAL_COMMERCIAL" as "socialCommercial",
        $"STRATEGIC_CHANNEL" as "strategicChannel",
        $"GLOBAL_CHANNEL" as "globalChannel",
        $"GLOBAL_SUBCHANNEL" as "globalSubChannel"
      )
      .as[ChannelMapping]
  }

}
