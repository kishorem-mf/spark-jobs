package com.unilever.ohub.spark

import com.unilever.ohub.spark.storage.{ DefaultStorage, Storage }
import org.apache.log4j.{ LogManager, Logger }
import org.apache.spark.sql.SparkSession
import scopt.OptionParser

trait SparkJobConfig extends Product
case class DefaultConfig(inputFile: String = "path-to-input-file", outputFile: String = "path-to-output-file") extends SparkJobConfig

trait SparkJob[Config <: SparkJobConfig] { self ⇒

  implicit protected val log: Logger = LogManager.getLogger(self.getClass)

  private[spark] def defaultConfig: Config

  private[spark] def configParser(): OptionParser[Config]

  def run(spark: SparkSession, config: Config, storage: Storage): Unit

  private[spark] def invokeRunWithConfig(config: Config): Unit = {
    val jobName = self.getClass.getSimpleName

    val spark = SparkSession
      .builder()
      .appName(jobName)
      .getOrCreate()

    val storage = new DefaultStorage(spark)
    val startOfJob = System.currentTimeMillis()

    run(spark, config, storage)

    log.info(s"Done in ${(System.currentTimeMillis - startOfJob) / 1000}s")
  }

  def main(args: Array[String]): Unit = {
    configParser().parse(args, defaultConfig) match {
      case Some(config) ⇒
        invokeRunWithConfig(config)
      case None ⇒ // nothing to do here
    }
  }
}

trait SparkJobWithDefaultConfig extends SparkJob[DefaultConfig] {
  override private[spark] def defaultConfig = DefaultConfig()

  override private[spark] def configParser(): OptionParser[DefaultConfig] =
    new scopt.OptionParser[DefaultConfig]("Spark job default") {
      head("run a spark job with default config.", "1.0")
      opt[String]("inputFile") required () action { (x, c) ⇒
        c.copy(inputFile = x)
      } text "inputFile is a string property"
      opt[String]("outputFile") required () action { (x, c) ⇒
        c.copy(outputFile = x)
      } text "outputFile is a string property"

      version("1.0")
      help("help") text "help text"
    }
}
