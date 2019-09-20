package com.unilever.ohub.spark.export.azuredw

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.entity._
import com.unilever.ohub.spark.domain.{DomainEntity, DomainEntityUtils}
import com.unilever.ohub.spark.storage.{DBConfig, Storage}
import com.unilever.ohub.spark.{SparkJob, SparkJobConfig}
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import scopt.OptionParser

import scala.reflect.runtime.universe._

/**
  * Configuration for the Azure Datawarehouse
  *
  * @param integratedInputFile
  * @param entityName : Tables in the datawarehouse will have the same entity name
  * @param dbUrl      : jdbc url, dafault value is "jdbc:sqlserver://ufs-marketing.database.windows.net:1433;database=ufs-marketing;",
  * @param dbUsername
  * @param dbPassword
  * @param dbSchema   : target schema, default is "ohub2",
  * @param dbTempDir  : temp bucket "wasbs://outbound@ohub2storagedev.blob.core.windows.net/DW"
  *
  */
case class AzureDWConfiguration(
                                 integratedInputFile: String = "integrated-input-file",
                                 entityName: String = "entity-name",
                                 dbUrl: String = "jdbc:sqlserver://ufs-marketing.database.windows.net:1433;database=ufs-marketing;",
                                 dbUsername: String = "db-username",
                                 dbPassword: String = "db-password",
                                 dbSchema: String = "ohub2",
                                 dbTempDir: String = "wasbs://outbound@ohub2storagedev.blob.core.windows.net/DW",
                                 blobStorageContainer: String = "ohub2storagedev",
                                 blobStorageKey: String = ""
                               ) extends SparkJobConfig

case class LogRow(
                   entityName: String,
                   loadingDate: java.sql.Timestamp,
                   latestContentDate: java.sql.Timestamp,
                   loadingTimeSec: Int,
                   rowCount: Int
                 )

abstract class SparkJobWithAzureDWConfiguration extends SparkJob[AzureDWConfiguration] {

  override private[spark] def defaultConfig = AzureDWConfiguration()

  override private[spark] def configParser(): OptionParser[AzureDWConfiguration] =
    new scopt.OptionParser[AzureDWConfiguration]("Spark job default") {
      head("run a spark job with default config.", "1.0")

      opt[String]("integratedInputFile") required() action { (x, c) ⇒
        c.copy(integratedInputFile = x)
      } text "integratedInputFile is a string property"
      opt[String]("entityName") optional() action { (x, c) ⇒
        c.copy(entityName = x)
      } text "dbTable is a string property"
      opt[String]("dbUrl") required() action { (x, c) ⇒
        c.copy(dbUrl = x)
      } text "dbUrl is a string property"
      opt[String]("dbUsername") required() action { (x, c) ⇒
        c.copy(dbUsername = x)
      } text "dbUsername is a string property"
      opt[String]("dbPassword") required() action { (x, c) ⇒
        c.copy(dbPassword = x)
      } text "dbPassword is a string property"
      opt[String]("dbSchema") required() action { (x, c) ⇒
        c.copy(dbSchema = x)
      } text "dbSchema is the target schema. Default value is ohub2"
      opt[String]("dbTempDir") required() action { (x, c) ⇒
        c.copy(dbTempDir = x)
      } text "dbTempDir the temporary loading bucket"
      opt[String]("blobStorageContainer") required() action { (x, c) ⇒
        c.copy(blobStorageContainer = x)
      } text "blobStorageContainer"
      opt[String]("blobStorageKey") required() action { (x, c) ⇒
        c.copy(blobStorageKey = x)
      } text "blobStorageKey"

      version("1.0")
      help("help") text "help text"
    }
}

abstract class AzureDWWriter[DomainType <: DomainEntity : TypeTag] extends SparkJobWithAzureDWConfiguration {

  def updateDataFrame(dataSet: DataFrame): DataFrame = dataSet

  /** Removes the map fields because resulting on an error when queried in Azure DW. */
  private def dropUnnecessaryFields(dataSet: Dataset[DomainType]): DataFrame = {

    val mapFields: Array[String] = dataSet.schema.fields.collect(
      { case field if field.dataType.typeName == "map" || field.dataType.typeName == "array" ⇒ field.name })

    val otherUnnecessaryFields = Seq("id")

    dataSet.drop(mapFields ++ otherUnnecessaryFields: _*)
  }

  private def logToAzureDWTable(spark: SparkSession, storage: Storage, config: AzureDWConfiguration, jobDuration: Int): Unit = {

    import spark.implicits._

    val insertedRowCount = storage.readAzureDWQuery(
      spark = spark, dbUrl = config.dbUrl, userName = config.dbUsername,
      userPassword = config.dbPassword, dbTempDir = config.dbTempDir,
      query = s"select count(*) as insertedRowCount from ${config.dbSchema}.${config.entityName}"
    ).select("insertedRowCount").rdd.map(r ⇒ r(0)).collect()(0).toString.toInt

    val maxOhubUpdated = storage.readAzureDWQuery(
      spark = spark, dbUrl = config.dbUrl, userName = config.dbUsername,
      userPassword = config.dbPassword, dbTempDir = config.dbTempDir,
      query = s"select isnull(max(ohubUpdated), '2019-01-01') as maxOhubUpdated from ${config.dbSchema}.${config.entityName}"
    ).select("maxOhubUpdated").rdd.map(r ⇒ r(0)).collect()(0).toString
    // TODO: review the logging mechanism

    val loggingDF = Seq(LogRow(
      entityName = config.entityName,
      loadingDate = new Timestamp(System.currentTimeMillis()),
      latestContentDate = Timestamp.valueOf(maxOhubUpdated),
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
    *                In order to satisfy the [flexibility] they need to be dropped and recreated together with the table.
    *                These "contraints" require the countryCode column to be always available.
    *
    */
  override def run(spark: SparkSession, config: AzureDWConfiguration, storage: Storage): Unit = {

    log.info(s"Writing integrated entities [${config.integratedInputFile}] to Azure DW in the table [${config.entityName}].")

    // Add blob storage key to hadoop configuration for wasb protocol connection required by the tempo staging directory
    spark.sparkContext.hadoopConfiguration.set(s"fs.azure.account.key.${config.blobStorageContainer}.blob.core.windows.net", config.blobStorageKey)

    val integratedEntity: Dataset[DomainType] = storage.readFromParquet[DomainType](config.integratedInputFile)
    val frame = dropUnnecessaryFields(integratedEntity)
    val result = updateDataFrame(frame)
    val dbTable: String = config.entityName
    val dbSchema: String = config.dbSchema
    val dbFullTableName: String = Seq(dbSchema, dbTable).mkString(".")
    // The row-level function is defined on the security schema, together with the policies
    val securityPolicyObject: String = s"Security.McoPolicy_ohub2_${dbTable}"
    val securityFunctionObject: String = s"Security.fn_securitypredicate_mco"
    val dropRowLevelSecurityPolicyAction: String = s"DROP SECURITY POLICY IF EXISTS ${securityPolicyObject}"
    val createRowLevelSecurityPolicyAction: String =
      s"""CREATE SECURITY POLICY ${securityPolicyObject}
          ADD FILTER PREDICATE ${securityFunctionObject}(countryCode)
          ON ${dbFullTableName}
          WITH (STATE = ON);
        """

    log.info(s"Destination table name: ${dbFullTableName}")
    log.info(s"Entity name: ${integratedEntity}")
    log.info(s"Pre action: ${dropRowLevelSecurityPolicyAction}")
    log.info(s"Post action: ${createRowLevelSecurityPolicyAction}")

    val startOfJob = System.currentTimeMillis()

    storage.writeAzureDWTable(
      df = result,
      DBConfig(dbUrl = config.dbUrl,
        dbTable = dbFullTableName,
        userName = config.dbUsername,
        userPassword = config.dbPassword,
        dbTempDir = config.dbTempDir),
      preActions = dropRowLevelSecurityPolicyAction,
      postActions = createRowLevelSecurityPolicyAction,
      saveMode = SaveMode.Overwrite
    )

    val jobDuration = ((System.currentTimeMillis - startOfJob) / 1000).toInt
    log.info(s"Written to ${dbFullTableName}")

    // Logging info to Azure DW
    logToAzureDWTable(spark, storage, config, jobDuration)

  }
}

object ActivityDWWriter extends AzureDWWriter[Activity]

object AnswerDWWriter extends AzureDWWriter[Answer]

object CampaignDWWriter extends AzureDWWriter[Campaign]

object CampaignBounceDWWriter extends AzureDWWriter[CampaignBounce]

object CampaignClickDWWriter extends AzureDWWriter[CampaignClick]

object CampaignOpenDWWriter extends AzureDWWriter[CampaignOpen]

object CampaignSendDWWriter extends AzureDWWriter[CampaignSend]

object ChannelMappingDWWriter extends AzureDWWriter[ChannelMapping]

object ContactPersonDWWriter extends AzureDWWriter[ContactPerson] {
  def updateDataFrame(spark: SparkSession, dataFrame: DataFrame): DataFrame = {
    import spark.implicits._

    dataFrame.withColumn("dateUpdated", when($"dateUpdated".isNotNull, $"dateUpdated").otherwise($"dateCreated"))
  }
}

object LoyaltyPointsDWWriter extends AzureDWWriter[LoyaltyPoints]

object OperatorDWWriter extends AzureDWWriter[Operator] {

  def updateDataFrame(spark: SparkSession, dataFrame: DataFrame): DataFrame = {
    import spark.implicits._

    dataFrame.withColumn("dateUpdated", when($"dateUpdated".isNotNull, $"dateUpdated").otherwise($"dateCreated"))
  }
}

object OrderDWWriter extends AzureDWWriter[Order]

object OrderLineDWWriter extends AzureDWWriter[OrderLine]

object ProductDWWriter extends AzureDWWriter[Product]

object QuestionDWWriter extends AzureDWWriter[Question]

object SubscriptionDWWriter extends AzureDWWriter[Subscription]

object ChainDWWriter extends AzureDWWriter[Chain]

/**
  * Runs concrete [[com.unilever.ohub.spark.export.azuredw.AzureDWWriter]]'s run method for all
  * [[com.unilever.ohub.spark.domain.DomainEntity]]s azureDwWriter values.
  *
  * When running this job, do bear in mind that the input location is now a folder, the entity name will be appended to it
  * to determine the location and the target table.
  *
  * F.e. to export data from runId "2019-08-06" provide "integratedInputFile" as:
  * "dbfs:/mnt/engine/integrated/2019-08-06"
  * In this case CP will be fetched from:
  * "dbfs:/mnt/engine/integrated/2019-08-06/contactpersons.parquet"
  **/
object AllDWOutboundWriter extends SparkJobWithAzureDWConfiguration {
  override def run(spark: SparkSession, config: AzureDWConfiguration, storage: Storage): Unit = {

    val excludedEntities = Seq("answers")

    DomainEntityUtils.domainCompanionObjects
      .par
      .filter(_.azureDwWriter.isDefined)
      .filterNot(entity => excludedEntities.contains(entity.engineFolderName))
      .foreach((entity) => {
        val writer = entity.azureDwWriter.get
        val integratedLocation = s"${config.integratedInputFile}/${entity.engineFolderName}.parquet"
        writer.run(spark, config.copy(integratedInputFile = integratedLocation, entityName = entity.engineFolderName), storage)
      })
  }
}
