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
    import spark.implicits._

    log.info(s"Initialize integrated parquet [${config.outputFile}] with empty dataset.")

    writeEmptyParquet(spark, storage, config.outputFile)
  }
}

object OperatorEmptyIntegratedWriter extends BaseEmptyIntegratedWriter[Operator] with OperatorEmptyParquetWriter

object ContactPersonEmptyIntegratedWriter extends BaseEmptyIntegratedWriter[ContactPerson] with ContactPersonEmptyParquetWriter

object SubscriptionEmptyIntegratedWriter extends BaseEmptyIntegratedWriter[Subscription] with SubscriptionEmptyParquetWriter

object ProductEmptyIntegratedWriter extends BaseEmptyIntegratedWriter[Product] with ProductEmptyParquetWriter

object OrderEmptyIntegratedWriter extends BaseEmptyIntegratedWriter[Order] with OrderEmptyParquetWriter

object OrderLineEmptyIntegratedWriter extends BaseEmptyIntegratedWriter[OrderLine] with OrderLineEmptyParquetWriter
