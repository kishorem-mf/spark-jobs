package com.unilever.ohub.spark.insights

import com.unilever.ohub.spark.SparkJobConfig
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{expr, upper, when}

trait DatabaseConfig extends SparkJobConfig {
  val databaseUrl: String = "databaseUrl"
  val databaseUserName: String = "databaseUserName"
  val databasePassword: String = "databasePassword"
}

trait DatabaseUtils {

  protected def getAuditTrailData(storage: Storage, query:String, config: DatabaseConfig)(implicit spark: SparkSession) = {
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

  protected def getErrorsData(storage: Storage, config: DatabaseConfig)(implicit spark: SparkSession) : DataFrame = {
    val query = InsightConstants.FILE_UPLOAD_ERROR_QUERY
    storage.readJdbcBasedOnQuery(config.databaseUrl, query, config.databaseUserName, config.databasePassword)
  }

  def getAdGroupUserData(storage: Storage, config: DatabaseConfig)(implicit spark: SparkSession) : DataFrame = {
    val query = InsightConstants.AD_GROUP_USERS_QUERY
    storage.readJdbcBasedOnQuery(config.databaseUrl, query, config.databaseUserName, config.databasePassword)
  }

  def getBatchJobExecutionData(storage: Storage, config: DatabaseConfig)(implicit spark: SparkSession) : DataFrame = {
    val query = InsightConstants.BATCH_JOB_EXEC_QUERY
    storage.readJdbcBasedOnQuery(config.databaseUrl, query, config.databaseUserName, config.databasePassword)
  }

  def getBatchStepExecutionData(storage: Storage, config: DatabaseConfig)(implicit spark: SparkSession) : DataFrame = {
    val query = InsightConstants.BATCH_STEP_QUERY
    storage.readJdbcBasedOnQuery(config.databaseUrl, query, config.databaseUserName, config.databasePassword)
  }

  def getSpnData(storage: Storage, config: DatabaseConfig)(implicit spark: SparkSession) : DataFrame = {
    val query = InsightConstants.SPN_QUERY
    storage.readJdbcBasedOnQuery(config.databaseUrl, query, config.databaseUserName, config.databasePassword)
  }
}
