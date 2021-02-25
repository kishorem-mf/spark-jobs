package com.unilever.ohub.spark.export.businessdatalake

import com.unilever.ohub.spark.domain.entity._
import com.unilever.ohub.spark.domain.{DomainEntity, DomainEntityUtils}
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.{SparkJob, SparkJobConfig}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import org.spark_project.dmg.pmml.True
import scopt.OptionParser

import scala.Function.chain
import scala.reflect.runtime.universe._
import scala.util.Try

/**
  *
  * @param integratedInputFile
  * @param outboundLocation
  * @param countryCodes
  */
case class DataLakeConfig(
                           integratedInputFile: String = "integrated-input-file",
                           entityName: String = "entity-name",
                           outboundLocation: String = "outbound-location",
                           countryCodes: String = "GB;IE"
                         ) extends SparkJobConfig

abstract class SparkJobWithAzureDLConfiguration extends SparkJob[DataLakeConfig] {

  override private[spark] def defaultConfig = DataLakeConfig()

  override private[spark] def configParser(): OptionParser[DataLakeConfig] =
    new scopt.OptionParser[DataLakeConfig]("Spark job default") {
      head("run a spark job with default config.", "1.0")

      opt[String]("integratedInputFile") required() action { (x, c) ⇒
        c.copy(integratedInputFile = x)
      } text "integratedInputFile is a string property"
      opt[String]("entityName") required() action { (x, c) ⇒
        c.copy(entityName = x)
      } text "entityName is a string property"
      opt[String]("outboundLocation") optional() action { (x, c) ⇒
        c.copy(outboundLocation = x)
      } text "outboundLocation is a string property"
      opt[String]("countryCodes") required() action { (x, c) ⇒
        c.copy(countryCodes = x)
      } text "countryCodes is a string property"

      version("1.0")
      help("help") text "help text"
    }

}

abstract class AzureDLWriter[DomainType <: DomainEntity : TypeTag] extends SparkJobWithAzureDLConfiguration {

   def splitAndWriteParquetFiles(entityName:String, inputData: Dataset[DomainType], folderDate: String, config: DataLakeConfig, spark:SparkSession, storage: Storage):String = {
    import spark.implicits._

    val splitArrayHyphen=folderDate.split("-")
    val splitArrayHyphenIndex=splitArrayHyphen.length-1
    val splitArraySlash=splitArrayHyphen(splitArrayHyphenIndex-2).split("/")
    val year = splitArraySlash(splitArraySlash.length-1)
    val month = splitArrayHyphen(splitArrayHyphenIndex-1)
    val day = splitArrayHyphen(splitArrayHyphenIndex).split("/")(0)
    var result = ""

    //If there are no records and only headers are present then we dont want to write any file
    config.countryCodes.split(";").foreach {
      country =>
          val location = config.outboundLocation + "/" + country.toLowerCase + "/" + entityName + "/Processed/YYYY=" + year + "/MM=" + month + "/DD=" + day + s"/${entityName}.parquet"
          val filterDs = inputData.filter($"concatId".startsWith(country + "~"))
          if(filterDs.count()>0){
            Try {
              storage.writeToParquet(filterDs, location, Seq(), SaveMode.Overwrite)
              result="Success"
            }.getOrElse(result)}
    }
    result
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
  override def run(spark: SparkSession, config: DataLakeConfig, storage: Storage): Unit = {

    log.info(s"Writing integrated entities [${config.integratedInputFile}] " +
      s"to Azure Data Lake.")
    val integData:Dataset[DomainType]=storage.readFromParquet(config.integratedInputFile)
    splitAndWriteParquetFiles(config.entityName,integData,
      config.integratedInputFile,config,spark,storage)
  }
}
object ActivityDLWriter extends AzureDLWriter[Activity]

object AssetDLWriter extends AzureDLWriter[Asset]

object AnswerDLWriter extends AzureDLWriter[Answer]

object CampaignDLWriter extends AzureDLWriter[Campaign]

object CampaignBounceDLWriter extends AzureDLWriter[CampaignBounce]

object CampaignClickDLWriter extends AzureDLWriter[CampaignClick]

object CampaignOpenDLWriter extends AzureDLWriter[CampaignOpen]

object CampaignSendDLWriter extends AzureDLWriter[CampaignSend]

object ChannelMappingDLWriter extends AzureDLWriter[ChannelMapping]

object ContactPersonDLWriter extends AzureDLWriter[ContactPerson]

object LoyaltyPointsDLWriter extends AzureDLWriter[LoyaltyPoints]

object OperatorDLWriter extends AzureDLWriter[Operator]

object OrderDLWriter extends AzureDLWriter[Order]

object OrderLineDLWriter extends AzureDLWriter[OrderLine]

object ProductDLWriter extends AzureDLWriter[Product]

object QuestionDLWriter extends AzureDLWriter[Question]

object SubscriptionDLWriter extends AzureDLWriter[Subscription]

object ChainDLWriter extends AzureDLWriter[Chain]

object ContactPersonGoldenDLWriter extends AzureDLWriter[ContactPersonGolden]

object OperatorGoldenDLWriter extends AzureDLWriter[OperatorGolden]

object OperatorRexLiteDLWriter extends AzureDLWriter[OperatorRexLite]

object ContactPersonRexLiteDLWriter extends AzureDLWriter[ContactPersonRexLite]

object OperatorChangeLogDLWriter extends AzureDLWriter[OperatorChangeLog]

object ContactPersonChangeLogDLWriter extends AzureDLWriter[ContactPersonChangeLog]

object AllDLOutboundWriter extends SparkJobWithAzureDLConfiguration {
  override def run(spark: SparkSession, config: DataLakeConfig, storage: Storage): Unit = {
    DomainEntityUtils.domainCompanionObjects
      .par
      .filter(_.dataLakeWriter.isDefined)
      .foreach((entity) => {
        val writer = entity.dataLakeWriter.get
        val integratedLocation = s"${config.integratedInputFile}${entity.engineFolderName}.parquet"
        val outboundPath =
          if (entity.auroraFolderLocation == Some("Restricted")) {
            s"${config.outboundLocation}Restricted/"
          }
          else {s"${config.outboundLocation}/"}
        writer.run(spark, config.copy(integratedInputFile = integratedLocation,
          entityName = entity.engineFolderName,
          outboundLocation = outboundPath), storage)
      })
  }
}


