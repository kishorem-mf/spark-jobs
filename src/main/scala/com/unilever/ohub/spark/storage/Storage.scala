package com.unilever.ohub.spark.storage

import java.util.{ Properties, UUID }

import org.apache.spark.sql.functions.col
import org.apache.spark.sql._
import com.unilever.ohub.spark.data.ChannelMapping
import com.unilever.ohub.spark.sql.JoinType
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.hadoop.fs._
import org.apache.log4j.Logger

trait Storage {

  def readFromCsv(
    location: String,
    fieldSeparator: String,
    hasHeaders: Boolean = true
  ): Dataset[Row]

  def writeToSingleCsv(
    ds: Dataset[_],
    outputFile: String,
    delim: String = ";",
    quote: String = "\""
  )(implicit log: Logger): Unit

  def readFromParquet[T: Encoder](location: String, selectColumns: Seq[Column] = Seq()): Dataset[T]

  def writeToParquet(ds: Dataset[_], location: String, partitionBy: Seq[String] = Seq()): Unit

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
      .csv(location)
  }

  def writeToSingleCsv(
    ds: Dataset[_],
    outputFile: String,
    delim: String = ";",
    quote: String = "\""
  )(implicit log: Logger): Unit = {

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val temporaryPath: String = new Path(new Path(outputFile).getParent, UUID.randomUUID().toString).toString

    val concatAvailable = isConcatAvailable(fs)
    val updatedDs = if (concatAvailable) ds else ds.coalesce(1)

    updatedDs
      .write
      .mode(SaveMode.Overwrite)
      .option("encoding", "UTF-8")
      .option("header", "true")
      .option("quoteAll", "true")
      .option("delimiter", delim)
      .option("quote", quote)
      .csv(temporaryPath)

    val csvPaths = getCsvFilePaths(fs, new Path(temporaryPath))

    if (concatAvailable) {
      fs.concat(new Path(outputFile), csvPaths)
      fs.delete(new Path(temporaryPath), true)
    } else {
      if (csvPaths.length != 1) {
        log.error("the number of csv-files found is greater or smaller than 1, this is not supported")
        System.exit(1)
      }
      fs.rename(csvPaths.head, new Path(outputFile))
      fs.delete(new Path(temporaryPath), true)
    }
  }

  private[storage] def isConcatAvailable(fs: FileSystem) = {
    try {
      fs.concat(new Path("/tmp/"), Array.empty)
      true
    } catch {
      case _: UnsupportedOperationException ⇒ false
      case _: Throwable                     ⇒ true
    }
  }

  private[storage] def getCsvFilePaths(fs: FileSystem, path: Path): Array[Path] = {
    def toList(it: RemoteIterator[LocatedFileStatus], arr: List[Path] = List.empty): List[Path] = {
      if (it.hasNext) {
        val nextPath = it.next().getPath
        toList(it, arr :+ nextPath)
      } else arr
    }

    toList(fs.listFiles(path, true))
      .filter(_.getName.endsWith(".csv"))
      .toArray
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

  private def readJdbcTable(
    spark: SparkSession,
    dbUrl: String,
    dbName: String,
    dbTable: String,
    userName: String,
    userPassword: String
  ): DataFrame = {
    val dbFullConnectionString = s"jdbc:postgresql://$dbUrl:5432/$dbName?ssl=true"

    val connectionProperties = new Properties
    connectionProperties.put("user", userName)
    connectionProperties.put("password", userPassword)

    spark
      .read
      .option(JDBCOptions.JDBC_DRIVER_CLASS, "org.postgresql.Driver")
      .jdbc(dbFullConnectionString, dbTable, connectionProperties)
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
