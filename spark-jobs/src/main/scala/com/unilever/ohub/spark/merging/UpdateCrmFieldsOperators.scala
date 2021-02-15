package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.domain.entity.{Operator, Product}
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.{SparkJob, SparkJobConfig}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

case class UpdateOperatorsConfig(
                                 currentIntegratedOperators: String = "operators-input-file",
                                 outputFile: String = "path-to-output-file"
                               ) extends SparkJobConfig

object UpdateOperators extends SparkJob[UpdateOperatorsConfig] {
  override private[spark] def defaultConfig  = UpdateOperatorsConfig()

  override private[spark] def configParser() =
    new scopt.OptionParser[UpdateOperatorsConfig]("Update Operators") {
    head("merges operators into an integrated operators output file.", "1.0")
    opt[String]("currentIntegratedOperators") required() action { (x, c) ⇒
      c.copy(currentIntegratedOperators = x)
    } text "currentIntegratedOperators is a string property"
    opt[String]("outputFile") required() action { (x, c) ⇒
      c.copy(outputFile = x)
    } text "outputFile is a string property"

    version("1.0")
    help("help") text "help text"
  }

  override def run(spark: SparkSession, config: UpdateOperatorsConfig, storage: Storage): Unit = {
    val operators:Dataset[Operator] = storage.readFromParquet[Operator](config.currentIntegratedOperators)
    val operatorsDF:DataFrame = operators.toDF()
   // operatorsDF.withColumn("crmAccountId",operators.)
    operatorsDF
      .drop("crmFields")
      .write
      .parquet(config.outputFile)

  }
}
