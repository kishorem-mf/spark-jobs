package com.unilever.ohub.spark.export.azuredw

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.{DomainEntity, DomainEntityUtils}
import com.unilever.ohub.spark.storage.{DBConfig, Storage}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import scala.reflect.runtime.universe._

abstract class AzureDWWriterNoPolicy[DomainType <: DomainEntity : TypeTag] extends AzureDWWriter {

  /** Removes the map fields because resulting on an error when queried in Azure DW. */
  override protected def dropUnnecessaryFields(dataFrame: DataFrame): DataFrame = {

    val mapFields: Array[String] = dataFrame.schema.fields.collect(
      { case field if field.dataType.typeName == "map" || field.dataType.typeName == "array" ⇒ field.name })

    dataFrame.drop(mapFields: _*)
  }

  override def logToAzureDWTable(spark: SparkSession, storage: Storage, config: AzureDWConfiguration, jobDuration: Int): Unit = {

    import spark.implicits._

    var dbSchema=config.dbSchema
    if(config.entityName.contains("crm")){
      dbSchema="crm"
    }

    val insertedRowCount = storage.readAzureDWQuery(
      spark = spark, dbUrl = config.dbUrl, userName = config.dbUsername,
      userPassword = config.dbPassword, dbTempDir = config.dbTempDir,
      query = s"select count(*) as insertedRowCount from ${dbSchema}.${config.entityName}"
    ).select("insertedRowCount").rdd.map(r ⇒ r(0)).collect()(0).toString.toInt

    val loggingDF = Seq(LogRow(
      entityName = config.entityName,
      loadingDate = new Timestamp(System.currentTimeMillis()),
      latestContentDate = null, // scalastyle:ignore
      loadingTimeSec = jobDuration.toInt,
      rowCount = insertedRowCount.toString.toInt
    )).toDF()

    storage.writeAzureDWTable(
      df = loggingDF,
      DBConfig(dbUrl = config.dbUrl,
        dbTable = "eng.logging_staging",
        userName = config.dbUsername,
        userPassword = config.dbPassword,
        dbTempDir = config.dbTempDir),
      saveMode = SaveMode.Append
    )
  }

  /**
   * Writes to a Azure DW table
   *
   * @param spark   spark session
   * @param config  the configuration definition
   * @param storage the class defining the storage mechanism
   *
   *                Implementation notes:
   *                [Flexibility] Tables are dropped and recreated (as default, it can be changed) so that every new
   *                field in the source parquet is automatically created in the destination table.
   *                Tables are subdue to a row-level security policy (table-valued function + security policy).
   *                In order to satisfy the [flexibility] they need to be dropped and recreated together with the table
   *                These "contraints" require the countryCode column to be always available.
   *
   */
  override def run(spark: SparkSession, config: AzureDWConfiguration, storage: Storage): Unit = {

    log.info(s"Writing integrated entities [${config.integratedInputFile}] " +
      s"to Azure DW in the table [${config.entityName}].")

    // Add blob storage key to hadoop configuration for wasb protocol connection required by the tempo staging directory
    spark.sparkContext.hadoopConfiguration.set(
      s"fs.azure.account.key.${config.blobStorageContainer}.blob.core.windows.net", config.blobStorageKey)

    val integratedEntity: DataFrame = storage.readFromParquet[DomainType](config.integratedInputFile).toDF
    val result = transform(integratedEntity)

    var dbSchema=config.dbSchema
    if(config.entityName.contains("crm")){
      dbSchema="crm"
    }

    val dbFullTableName: String = Seq(dbSchema, config.entityName).mkString(".")

    log.info(s"Destination table name: ${dbFullTableName}")
    log.info(s"Entity name: ${integratedEntity}")

    val startOfJob = System.currentTimeMillis()

    storage.writeAzureDWTable(
      df = result,
      DBConfig(dbUrl = config.dbUrl,
        dbTable = dbFullTableName,
        userName = config.dbUsername,
        userPassword = config.dbPassword,
        dbTempDir = config.dbTempDir),
      saveMode = SaveMode.Overwrite
    )

    val jobDuration = ((System.currentTimeMillis - startOfJob) / 1000).toInt
    log.info(s"Written to ${dbFullTableName}")

    // Logging info to Azure DW
    logToAzureDWTable(spark, storage, config, jobDuration)

  }
}



