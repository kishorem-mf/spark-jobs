package com.unilever.ohub.spark.ingest.initial

import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.domain.entity._
import com.unilever.ohub.spark.ingest._
import com.unilever.ohub.spark.{ SparkJob, SparkJobConfig }
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.SparkSession
import scopt.OptionParser

import scala.reflect.runtime.universe._

case class EmptyIntegratedConfig(outputFile: String = "path-to-output-file") extends SparkJobConfig

/*
  The goal of this spark job is to write an empty typed dataset to parquet. It is used to bootstrap the delta process,
  since at the begin (t=0) there is no integrated parquet file (yet).
 */
abstract class BaseEmptyIntegratedWriter[DomainType <: DomainEntity: TypeTag] extends SparkJob[EmptyIntegratedConfig] with EmptyParquetWriter[DomainType] {

  override private[spark] def defaultConfig = EmptyIntegratedConfig()

  override private[spark] def configParser(): OptionParser[EmptyIntegratedConfig] =
    new scopt.OptionParser[EmptyIntegratedConfig]("Empty parquet writer") {
      head("writes an empty parquet file to bootstrap the delta process.", "1.0")

      opt[String]("outputFile") required () action { (x, c) ⇒
        c.copy(outputFile = x)
      } text "outputFile is a string property"

      version("1.0")
      help("help") text "help text"
    }

  override def run(spark: SparkSession, config: EmptyIntegratedConfig, storage: Storage): Unit = {
    log.info(s"Initialize integrated parquet [${config.outputFile}] with empty dataset.")
    writeEmptyParquet(spark, storage, config.outputFile)
  }
}

object ActivityEmptyIntegratedWriter extends BaseEmptyIntegratedWriter[Activity] with ActivityEmptyParquetWriter

object AnswerEmptyIntegratedWriter extends BaseEmptyIntegratedWriter[Answer] with AnswerEmptyParquetWriter

object AssetEmptyIntegratedWriter extends BaseEmptyIntegratedWriter[Asset] with AssetEmptyParquetWriter

object CampaignEmptyIntegratedWriter extends BaseEmptyIntegratedWriter[Campaign] with CampaignEmptyParquetWriter

object CampaignBounceEmptyIntegratedWriter extends BaseEmptyIntegratedWriter[CampaignBounce] with CampaignBounceEmptyParquetWriter

object CampaignClickEmptyIntegratedWriter extends BaseEmptyIntegratedWriter[CampaignClick] with CampaignClickEmptyParquetWriter

object CampaignOpenEmptyIntegratedWriter extends BaseEmptyIntegratedWriter[CampaignOpen] with CampaignOpenEmptyParquetWriter

object CampaignSendEmptyIntegratedWriter extends BaseEmptyIntegratedWriter[CampaignSend] with CampaignSendEmptyParquetWriter

object ChainEmptyIntegratedWriter extends BaseEmptyIntegratedWriter[Chain] with ChainEmptyParquetWriter

object ChannelMappingEmptyIntegratedWriter extends BaseEmptyIntegratedWriter[ChannelMapping] with ChannelMappingEmptyParquetWriter

object ContactPersonEmptyIntegratedWriter extends BaseEmptyIntegratedWriter[ContactPerson] with ContactPersonEmptyParquetWriter

object LoyaltyPointsEmptyIntegratedWriter extends BaseEmptyIntegratedWriter[LoyaltyPoints] with LoyaltyPointsEmptyParquetWriter

object OperatorEmptyIntegratedWriter extends BaseEmptyIntegratedWriter[Operator] with OperatorEmptyParquetWriter

object OrderEmptyIntegratedWriter extends BaseEmptyIntegratedWriter[Order] with OrderEmptyParquetWriter

object OrderLineEmptyIntegratedWriter extends BaseEmptyIntegratedWriter[OrderLine] with OrderLineEmptyParquetWriter

object ProductEmptyIntegratedWriter extends BaseEmptyIntegratedWriter[Product] with ProductEmptyParquetWriter

object QuestionEmptyIntegratedWriter extends BaseEmptyIntegratedWriter[Question] with QuestionEmptyParquetWriter

object SubscriptionEmptyIntegratedWriter extends BaseEmptyIntegratedWriter[Subscription] with SubscriptionEmptyParquetWriter

object OperatorChangeLogEmptyIntegratedWriter extends BaseEmptyIntegratedWriter[OperatorChangeLog] with OperatorChangeLogEmptyParquetWriter

object ContactPersonChangeLogEmptyIntegratedWriter extends BaseEmptyIntegratedWriter[ContactPersonChangeLog] with ContactPersonChangeLogEmptyParquetWriter

object ContactPersonRexLiteEmptyIntegratedWriter extends BaseEmptyIntegratedWriter[ContactPersonRexLite] with ContactPersonRexLiteEmptyParquetWriter

object OperatorRexLiteEmptyIntegratedWriter extends BaseEmptyIntegratedWriter[OperatorRexLite] with OperatorRexLiteEmptyParquetWriter

object ContactPersonGoldenEmptyIntegratedWriter extends BaseEmptyIntegratedWriter[ContactPersonGolden] with ContactPersonGoldenEmptyParquetWriter

object OperatorGoldenEmptyIntegratedWriter extends BaseEmptyIntegratedWriter[OperatorGolden] with OperatorGoldenEmptyParquetWriter
