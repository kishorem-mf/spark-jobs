package com.unilever.ohub.spark.insights

import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import scopt.OptionParser

case class FileUploadErrorsConfig(
                                 execDate: String = "executionDate",
                                 override val incomingRawPath: String = "incomingRawPath",
                                 override val previousInsightsPath: String = "previousInsightsPath",
                                 override val insightsOutputPath: String = "insightsOutputPath",
                                 override val databaseUrl: String = "databaseUrl",
                                 override val databaseUserName:String = "databaseUserName",
                                 override val databasePassword:String = "databasePassword"
                                 ) extends BaseInsightConfig

object FileUploadErrorInsigts extends SparkJob[FileUploadErrorsConfig] {

  override private[spark] def defaultConfig = FileUploadErrorsConfig()

  override private[spark] def configParser(): OptionParser[FileUploadErrorsConfig] =
    new scopt.OptionParser[FileUploadErrorsConfig]("get File Upload Error Insights") {
      head("change log entity output file.", "1.0")
      opt[String]("execDate") required() action { (x, c) ⇒
        c.copy(execDate = x)
      } text("Execution date to filter")
      opt[String]("incomingRawPath") required() action { (x, c) ⇒
        c.copy(incomingRawPath = x)
      } text("Path of Incoming blob folder")
      opt[String]("previousInsightsPath") required() action { (x, c) ⇒
        c.copy(previousInsightsPath = x)
      } text("Previous insight blob file path")
      opt[String]("insightsOutputPath") required() action { (x, c) ⇒
        c.copy(insightsOutputPath = x)
      } text("Output blob path")
      opt[String]("databaseUrl") required() action { (x, c) ⇒
        c.copy(databaseUrl = x)
      } text("URL of database")
      opt[String]("databaseUserName") required() action { (x, c) ⇒
        c.copy(databaseUserName = x)
      } text("Username of database")
      opt[String]("databasePassword") required() action { (x, c) ⇒
        c.copy(databasePassword = x)
      } text("Password of Database")

      version("1.0")
      help("help") text "help text"
    }

  def getAllRequiredDF(config: FileUploadErrorsConfig, storage: Storage)(implicit spark: SparkSession)
  : (DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame) = {
    import spark.implicits._

    val auditTrailDF = BaseInsightUtils.getAuditTrailsForFileUploadErrorsInsights(storage, config)
      .filter(date_format($"CREATED_DATE", "yyyy-MM-dd") === config.execDate)
      .withColumnRenamed("USER_NAME", "AUDIT_USER_NAME")

    val auditTrailFileList = auditTrailDF.select("AUDIT_FILE_NAME").rdd.map(r => r(0).toString()).collect.toSeq

    val errorDF = BaseInsightUtils.getErrorsDataForFileUploadInsights(storage, config, auditTrailFileList)

    val batchJobDF = BaseInsightUtils.getBatchJobExecutionData(storage, config)
      .filter($"string_val".isin(auditTrailFileList:_*))
      .select("job_execution_id", "string_val")

    val jobExecIds = batchJobDF.select("job_execution_id").rdd.map(r => r(0).toString()).collect.toSeq

    val batchStepDF = BaseInsightUtils.getBatchStepExecutionData(storage, config)
      .filter($"step_name" === "model_step")
      .filter($"job_execution_id".isin(jobExecIds:_*))
      .select("job_execution_id", "read_count")

    val adGroupDF = BaseInsightUtils.getAdGroupUserData(storage, config)
      .withColumnRenamed("username", "ad_user_name")

    val spnDF = BaseInsightUtils.getSpnData(storage, config)

    (auditTrailDF, errorDF, batchJobDF, batchStepDF, adGroupDF, spnDF)
  }

  def transform(config: FileUploadErrorsConfig, storage: Storage)(implicit spark: SparkSession) : DataFrame = {

    import spark.implicits._
    val fileUploadInsightColumns = InsightConstants.FILE_UPLOAD_ERROR_COLUMNS
    val (auditTrailDF, errorDF, batchJobDF, batchStepDF, adGroupDF, spnDF) = getAllRequiredDF(config, storage)

    auditTrailDF
    .join(batchJobDF, auditTrailDF("AUDIT_FILE_NAME") === batchJobDF("string_val"), "inner")
    .join(batchStepDF, batchJobDF("job_execution_id") === batchStepDF("job_execution_id"), "inner")
    .join(adGroupDF, auditTrailDF("AUDIT_USER_NAME") === adGroupDF("ad_user_name"), "left")
    .join(spnDF, auditTrailDF("AUDIT_USER_NAME") === spnDF("spnid"), "left")
    .join(errorDF, auditTrailDF("AUDIT_FILE_NAME") === errorDF("error_file_name"), "left")
    .withColumn("USER_NAME", when(length($"AUDIT_USER_NAME")  - length(regexp_replace($"AUDIT_USER_NAME", "-", "")) === 4, $"spnname") otherwise($"AUDIT_USER_NAME"))
    .withColumn("COUNTRY",
      when($"spnname" contains("svc"),
        expr("reverse(substring(reverse(spnname), 1, length(substring_index(reverse(spnname), '-', 1))))"))
        otherwise(when($"country" isNull, lit("NL")) otherwise($"country")))


    .withColumnRenamed("AUDIT_FILE_NAME", "FILE_NAME")
    .withColumnRenamed("read_count", "TOTAL_RECORDS")
    .withColumnRenamed("CREATED_DATE", "TIMESTAMP")
    .withColumn("IS_WARNING",  col("IS_WARNING").cast("String"))
    .withColumn("SOURCE_NAME", BaseInsightUtils.getSourceName($"FILE_NAME"))
    .withColumn("ENTITY_NAME", BaseInsightUtils.getModelName($"FILE_NAME"))
    .select(fileUploadInsightColumns.head, fileUploadInsightColumns.tail: _*)
  }

  override def run(spark: SparkSession, config: FileUploadErrorsConfig, storage: Storage): Unit = {

    log.info(s"Inside FileUploadErrorInsights with execDate :: ${config.execDate}")

    implicit val sparkSession:SparkSession = spark

    val previousIntegratedInsightsDF = storage.readFromCsv(config.previousInsightsPath, InsightConstants.SEMICOLON,
      true, InsightConstants.ESCAPE_BACKSLASH).cache()

    val deltaIntegratedInsightsDF = transform(config, storage)

    val integratedDF = deltaIntegratedInsightsDF.unionByName(previousIntegratedInsightsDF)

    BaseInsightUtils.writeToCsv(config.insightsOutputPath, InsightConstants.FILE_UPLOAD_ERRORS_FILENAME, integratedDF)

    log.info(s"Finished executing FileUploadErrorInsights with execDate :: ${config.execDate}")

  }
}
