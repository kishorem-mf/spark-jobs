package com.unilever.ohub.spark.insights

import java.net.URI
import java.util.UUID

import com.unilever.ohub.spark.SparkJobConfig
import com.unilever.ohub.spark.storage.Storage
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

trait BaseInsightConfig extends SparkJobConfig {
  val incomingRawPath: String = "incomingRawPath"
  val previousInsightsPath: String = "previousInsightsPath"
  val insightsOutputPath: String = "insightsOutputPath"
  val databaseUrl: String = "databaseUrl"
  val databaseUserName: String = "databaseUserName"
  val databasePassword: String = "databasePassword"
}

object BaseInsightUtils {

  val CSV_NAME_PATTERN_STRING= "UFS_([A-Z0-9_-]+)_" +
      "(CHAINS|LOYALTY|OPERATORS|OPERATOR_ACTIVITIES|OPERATOR_CLASSIFICATIONS|CONTACTPERSONS|CONTACTPERSON_ACTIVITIES|CONTACTPERSON_CLASSIFICATIONS|" +
      "CONTACTPERSON_SOCIAL|CONTACTPERSON_ANONYMIZATIONS|PRODUCTS|ORDERS|ORDERLINES|ORDERS_DELETED|ORDERLINES_DELETED|SUBSCRIPTIONS|QUESTIONS|ANSWERS|" +
      "CAMPAIGN_OPENS|CAMPAIGN_CLICKS|CAMPAIGN_BOUNCES|CAMPAIGNS|CAMPAIGN_SENDS|CHANNEL_MAPPINGS)" +
      "_([0-9]{14})"

  val CSV_NAME_PATTERN = CSV_NAME_PATTERN_STRING.r

  val getSourceName = udf((fileName: String) => getModelAndSourceName(fileName)._1)
  val getModelName  = udf((fileName: String) => getModelAndSourceName(fileName)._2)

  def getModelAndSourceName(csvFileName: String): (String, String) =  {
    csvFileName.toUpperCase() match {
      case CSV_NAME_PATTERN(sourceName,modelName,_) => (sourceName, modelName)
      case _ =>("-","-")
    }
  }

  def roundOffValue(input: Double): Double = {
    Math.round(input * 100.0) / 100.0
  }

  def getBaseName(path: String): String = {
    path.substring(path.lastIndexOf("UFS_"), path.lastIndexOf(".")).toUpperCase
  }

  private def getAuditTrailData(storage: Storage, query:String, config: BaseInsightConfig)(implicit spark: SparkSession) = {
    import spark.implicits._

    storage.readJdbcBasedOnQuery(config.databaseUrl, query, config.databaseUserName, config.databasePassword)
      .withColumn("file_name", upper($"file_name"))
      .withColumnRenamed("file_name", "AUDIT_FILE_NAME")
      .withColumnRenamed("timestamp", "CREATED_DATE")
      .withColumnRenamed("status", "STATUS")
      .withColumnRenamed("username", "USER_NAME")
      .withColumnRenamed("reason_failed", "REASON_FAILED")
      .withColumnRenamed("linked_file", "LINKED_FILE")
      .withColumn("USER_NAME", when($"USER_NAME".contains("svc-b-da-"), expr("substring(USER_NAME, 18, length(USER_NAME))")) otherwise($"USER_NAME"))
  }

  def getAuditTrailsForDataCompletenessInsights(storage: Storage, config: BaseInsightConfig)(implicit spark: SparkSession) : DataFrame = {
    val query = InsightConstants.AUDIT_TRAILS_QUERY
    getAuditTrailData(storage, query, config)
  }

  def getAuditTrailsForFileUploadErrorsInsights(storage: Storage, config: BaseInsightConfig)(implicit spark: SparkSession) : DataFrame = {
    val query = InsightConstants.FILE_UPLOADS_QUERY
    getAuditTrailData(storage, query, config)
  }

  private def getErrorsData(storage: Storage, config: BaseInsightConfig)(implicit spark: SparkSession) : DataFrame = {
    val query = InsightConstants.FILE_UPLOAD_ERROR_QUERY
    storage.readJdbcBasedOnQuery(config.databaseUrl, query, config.databaseUserName, config.databasePassword)
  }

  def getAdGroupUserData(storage: Storage, config: BaseInsightConfig)(implicit spark: SparkSession) : DataFrame = {
    val query = InsightConstants.AD_GROUP_USERS_QUERY
    storage.readJdbcBasedOnQuery(config.databaseUrl, query, config.databaseUserName, config.databasePassword)
  }

  def getBatchJobExecutionData(storage: Storage, config: BaseInsightConfig)(implicit spark: SparkSession) : DataFrame = {
    val query = InsightConstants.BATCH_JOB_EXEC_QUERY
    storage.readJdbcBasedOnQuery(config.databaseUrl, query, config.databaseUserName, config.databasePassword)
  }

  def getBatchStepExecutionData(storage: Storage, config: BaseInsightConfig)(implicit spark: SparkSession) : DataFrame = {
    val query = InsightConstants.BATCH_STEP_QUERY
    storage.readJdbcBasedOnQuery(config.databaseUrl, query, config.databaseUserName, config.databasePassword)
  }

  def getSpnData(storage: Storage, config: BaseInsightConfig)(implicit spark: SparkSession) : DataFrame = {
    val query = InsightConstants.SPN_QUERY
    storage.readJdbcBasedOnQuery(config.databaseUrl, query, config.databaseUserName, config.databasePassword)
  }

  def getErrorsDataForFileUploadInsights(storage: Storage, config: BaseInsightConfig, filesList: Seq[String])(implicit spark: SparkSession) : DataFrame = {
    import spark.implicits._

    val errorTypeWindow = Window.partitionBy($"file_name", $"is_warning", $"split_error")
    val orderedErrorTypeWindow = errorTypeWindow.orderBy($"error_message")

    getErrorsData(storage, config)
      .filter($"file_name".isin(filesList:_*))
      .withColumn("split_error", substring_index($"error_message", " ", 2))
      .withColumn("error_count", count($"split_error") over errorTypeWindow)
      .withColumn("rowNumber", row_number over orderedErrorTypeWindow)
      .filter('rowNumber  === 1)
      .drop("line", "rowNumber", "rejected_value", "line_number", "timestamp", "split_error")
      .withColumnRenamed("file_name", "error_file_name")
      .withColumnRenamed("error_count", "ERROR_TYPE_COUNT_PER_FILE")
      .withColumnRenamed("error_message", "ERROR_MESSAGE")
      .withColumnRenamed("is_warning", "IS_WARNING")
  }

  def listCsvFiles(basePath: String, storage: Storage)(implicit spark: SparkSession): Seq[String] = {
    val conf = new Configuration(spark.sparkContext.hadoopConfiguration)
    val fs = FileSystem.get(new URI(basePath), conf)
    storage.getCsvFilePaths(fs, new Path(basePath))
      .map(_.toString)
      .filter(path => path.substring(InsightConstants.BASE_FILE_NAME_INDEX, path.length) matches InsightConstants.DATA_COMPLETENESS_FILE_PATTERN)
  }


  def writeToCsv(path: String, fileName: String, ds: Dataset[_])(implicit spark: SparkSession): Unit = {
    val outputFolderPath = new Path(path)
    val temporaryPath = new Path(outputFolderPath, UUID.randomUUID().toString)
    val outputFilePath = new Path(outputFolderPath, s"${fileName}")
    val writeableData = ds
      .write
      .mode(SaveMode.Overwrite)
      .option("encoding", "UTF-8")
      .option("header", "false")
      .option("quoteAll", "true")
      .option("delimiter", ";")

    writeableData.csv(temporaryPath.toString)
    val header = ds.columns.map(c ⇒ "\"" + c + "\"").mkString(";")
    mergeDirectoryToOneFile(temporaryPath, outputFilePath, spark, header)

  }

  def mergeDirectoryToOneFile(sourceDirectory: Path, outputFile: Path, spark: SparkSession, header: String): Boolean = {

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    //Create a new header file which start with a `_` because the files are merged based on alphabetical order so
    //the header files will be merged first
    createHeaderFile(fs, sourceDirectory, header)


    def moveOnlyCsvFilesToOtherDirectory = {
      //Move all csv files to different directory so we don't make a mistake of merging other files from the source directory
      val tmpCsvSourceDirectory = new Path(sourceDirectory.getParent, UUID.randomUUID().toString)
      fs.mkdirs(tmpCsvSourceDirectory)
      fs.listStatus(sourceDirectory)
        .filter(p ⇒ p.isFile)
        .filter(p ⇒ p.getPath.getName.endsWith(".csv"))
        .map(_.getPath)
        .foreach(fs.rename(_, tmpCsvSourceDirectory))
      tmpCsvSourceDirectory
    }

    val tmpCsvSourceDirectory: Path = moveOnlyCsvFilesToOtherDirectory
    FileUtil.copyMerge(fs, tmpCsvSourceDirectory, fs, outputFile, true, spark.sparkContext.hadoopConfiguration, null) // scalastyle:ignore
    fs.delete(sourceDirectory, true)
  }

  def createHeaderFile(fs: FileSystem, sourceDirectory: Path, header: String): Path = {
    import java.io.{BufferedWriter, OutputStreamWriter}

    val headerFile = new Path(sourceDirectory, "_" + UUID.randomUUID().toString + ".csv")
    val out = fs.create(headerFile)
    val br = new BufferedWriter(new OutputStreamWriter(out, "UTF-8"))
    try {
      br.write(header + "\n")
    } finally {
      if (out != null) br.close()
    }
    headerFile
  }

}
