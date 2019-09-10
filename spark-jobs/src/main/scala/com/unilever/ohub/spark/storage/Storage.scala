package com.unilever.ohub.spark.storage

import java.util.Properties

import org.apache.hadoop.fs._
import org.apache.spark.sql._
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions

import scala.reflect.runtime.universe._

trait Storage {

  def readFromCsv(
    location: String,
    fieldSeparator: String,
    hasHeaders: Boolean = true
  ): Dataset[Row]

  def readFromParquet[T <: Product: TypeTag](location: String, selectColumns: Seq[Column] = Seq()): Dataset[T]

  def writeToParquet(ds: Dataset[_], location: String, partitionBy: Seq[String] = Seq(), saveMode: SaveMode = SaveMode.Overwrite): Unit

  def readJdbcTable(dbUrl: String, dbTable: String, userName: String, userPassword: String, jdbcDriverClass: String = "org.postgresql.Driver"): DataFrame

  def writeJdbcTable(df: DataFrame, dbUrl: String, dbTable: String, userName: String, userPassword: String,
    jdbcDriverClass: String = "org.postgresql.Driver", saveMode: SaveMode = SaveMode.Overwrite): Unit

  def writeAzureDWTable(
    df: DataFrame,
    dBConfig: DBConfig,
    preActions: String = "",
    postActions: String = "",
    saveMode: SaveMode = SaveMode.Overwrite
  ): Unit

  def readAzureDWQuery(
    spark: SparkSession,
    jdbcDriverClass: String = "com.databricks.spark.sqldw",
    dbUrl: String,
    userName: String,
    userPassword: String,
    dbTempDir: String,
    query: String
  ): DataFrame
}

class DefaultStorage(spark: SparkSession) extends Storage {

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
      .option("encoding", "UTF-8")
      .option("escape", "\"")
      .csv(location)
  }

  private[storage] def getCsvFilePaths(fs: FileSystem, path: Path): Array[Path] = {
    def toList(it: RemoteIterator[LocatedFileStatus], arr: List[Path] = List.empty): List[Path] = {
      if (it.hasNext) {
        val nextPath = it.next().getPath
        toList(it, arr :+ nextPath)
      } else {
        arr
      }
    }

    toList(fs.listFiles(path, true))
      .filter(_.getName.endsWith(".csv"))
      .toArray
  }

  override def readFromParquet[T <: Product: TypeTag](location: String, selectColumns: Seq[Column] = Seq()): Dataset[T] = {
    implicit val encoder = Encoders.product[T]

    val parquetDF = spark
      .read
      .schema(encoder.schema)
      .parquet(location)

    val parquetSelectDF = {
      if (selectColumns.nonEmpty) parquetDF.select(selectColumns: _*) else parquetDF
    }

    parquetSelectDF.as[T]
  }

  override def writeToParquet(ds: Dataset[_], location: String, partitionBy: Seq[String] = Seq(), saveMode: SaveMode = SaveMode.Overwrite): Unit = {
    ds
      .write
      .mode(saveMode)
      .partitionBy(partitionBy: _*)
      .parquet(location)
  }

  // see also: http://spark.apache.org/docs/latest/sql-programming-guide.html#jdbc-to-other-databases
  override def readJdbcTable(dbUrl: String, dbTable: String, userName: String, userPassword: String, jdbcDriverClass: String = "org.postgresql.Driver"): DataFrame = {
    spark
      .read
      .option(JDBCOptions.JDBC_DRIVER_CLASS, jdbcDriverClass)
      .jdbc(dbUrl, dbTable, connectionProperties(userName, userPassword))
  }

  override def writeJdbcTable(df: DataFrame, dbUrl: String, dbTable: String, userName: String, userPassword: String,
    jdbcDriverClass: String = "org.postgresql.Driver", saveMode: SaveMode = SaveMode.Overwrite): Unit = {
    df
      .write
      .mode(saveMode)
      .option(JDBCOptions.JDBC_DRIVER_CLASS, jdbcDriverClass)
      .option(JDBCOptions.JDBC_TRUNCATE, true) // when set to 'true' truncate table, otherwise drop & create table
      .jdbc(dbUrl, dbTable, connectionProperties(userName, userPassword))
  }

  private def connectionProperties(userName: String, userPassword: String) = {
    val connectionProperties = new Properties
    connectionProperties.put("user", userName)
    connectionProperties.put("password", userPassword)
    connectionProperties
  }

  /**
   * Configuration for the Azure Datawarehouse
   *
   * @param df
   * @param dBConfig a object of type DBConfig
   * @param preActions a sequence of statements separated by ; executed before writing
   * @param postActions a sequence of statements separated by ; executed after writing
   */
  override def writeAzureDWTable(
    df: DataFrame,
    dBConfig: DBConfig,
    preActions: String = "",
    postActions: String = "",
    saveMode: SaveMode = SaveMode.Overwrite
  ): Unit = {

    val fullDbURL: String = s"${dBConfig.dbUrl};user=${dBConfig.userName};password=${dBConfig.userPassword}"

    df
      .write
      .format(dBConfig.jdbcDriverClass)
      .option("url", fullDbURL)
      .option("forwardSparkAzureStorageCredentials", "true")
      .option("dbTable", dBConfig.dbTable)
      .option("tempDir", dBConfig.dbTempDir)
      .option("preActions", preActions)
      .option("postActions", postActions)
      .option("maxStrLength", 4000)  // scalastyle:ignore
      .mode(saveMode)
      .save
  }

  /**
   * Runs query on the datawarehouse
   *
   * @param spark
   * @param jdbcDriverClass
   * @param dbUrl        : example "jdbc:sqlserver://ufs-marketing.database.windows.net:1433;database=ufs-marketing;",
   * @param userName
   * @param userPassword :
   * @param dbTempDir    : temp bucket, wasb protocol only accepted
   *                    eg. "wasbs://outbound@ohub2storagedev.blob.core.windows.net/DW"
   * @param query        : the query in T-SQL dialect - Azure DW compatible
   */
  override def readAzureDWQuery(
    spark: SparkSession,
    jdbcDriverClass: String = "com.databricks.spark.sqldw",
    dbUrl: String,
    userName: String,
    userPassword: String,
    dbTempDir: String,
    query: String
  ): DataFrame = {

    val fullDbURL: String = s"${dbUrl};user=${userName};password=${userPassword}"

    spark.read
      .format(jdbcDriverClass)
      .option("url", fullDbURL)
      .option("forwardSparkAzureStorageCredentials", "true")
      .option("tempDir", dbTempDir)
      .option("query", query)
      .load()
  }
}

case class DBConfig(jdbcDriverClass:String = "com.databricks.spark.sqldw", dbUrl: String, dbTable:String, userName: String, userPassword:String, dbTempDir:String)
