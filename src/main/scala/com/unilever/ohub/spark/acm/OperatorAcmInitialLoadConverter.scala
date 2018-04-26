package com.unilever.ohub.spark.acm

import com.unilever.ohub.spark.{ SparkJob, SparkJobConfig }
import com.unilever.ohub.spark.acm.model.UFSOperator
import com.unilever.ohub.spark.data.ChannelMapping
import com.unilever.ohub.spark.domain.entity.Operator
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{ Dataset, SparkSession }
import scopt.OptionParser

case class DefaultWithDbConfig(
    inputFile: String = "path-to-input-file",
    outputFile: String = "path-to-output-file",
    postgressUrl: String = "postgress-url",
    postgressUsername: String = "postgress-username",
    postgressPassword: String = "postgress-password",
    postgressDB: String = "postgress-db"
) extends SparkJobConfig

object OperatorAcmInitialLoadConverter extends SparkJob[DefaultWithDbConfig] with OperatorAcmConverter {

  def transform(
    spark: SparkSession,
    channelMappings: Dataset[ChannelMapping],
    operators: Dataset[Operator]
  ): Dataset[UFSOperator] = {
    createUfsOperators(spark, operators, channelMappings)
  }

  override private[spark] def defaultConfig = DefaultWithDbConfig()

  override private[spark] def configParser(): OptionParser[DefaultWithDbConfig] =
    new scopt.OptionParser[DefaultWithDbConfig]("Operator ACM converter") {
      head("converts domain operators into ufs operators.", "1.0")
      opt[String]("inputFile") required () action { (x, c) ⇒
        c.copy(inputFile = x)
      } text "inputFile is a string property"
      opt[String]("outputFile") required () action { (x, c) ⇒
        c.copy(outputFile = x)
      } text "outputFile is a string property"
      opt[String]("postgressUrl") required () action { (x, c) ⇒
        c.copy(postgressUrl = x)
      } text "postgressUrl is a string property"
      opt[String]("postgressUsername") required () action { (x, c) ⇒
        c.copy(postgressUsername = x)
      } text "postgressUsername is a string property"
      opt[String]("postgressPassword") required () action { (x, c) ⇒
        c.copy(postgressPassword = x)
      } text "postgressPassword is a string property"
      opt[String]("postgressDB") required () action { (x, c) ⇒
        c.copy(postgressDB = x)
      } text "postgressDB is a string property"

      version("1.0")
      help("help") text "help text"
    }

  override def run(spark: SparkSession, config: DefaultWithDbConfig, storage: Storage): Unit = {
    import spark.implicits._

    log.info(s"Generating operator ACM csv file from [$config.inputFile] to [$config.outputFile]")

    val channelMappings = storage.channelMappings(config.postgressUrl, config.postgressDB, config.postgressUsername, config.postgressPassword)
    val operators = storage.readFromParquet[Operator](config.inputFile)
    val transformed = transform(spark, channelMappings, operators)

    writeToCsv(storage, transformed, config.outputFile)
  }
}
