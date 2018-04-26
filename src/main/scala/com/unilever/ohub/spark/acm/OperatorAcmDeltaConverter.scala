package com.unilever.ohub.spark.acm

import com.unilever.ohub.spark.{SparkJob, SparkJobConfig}
import com.unilever.ohub.spark.data.ChannelMapping
import com.unilever.ohub.spark.domain.entity.Operator
import com.unilever.ohub.spark.sql.JoinType
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{Dataset, SparkSession}
import scopt.OptionParser

case class DefaultWithDbAndDeltaConfig(
    inputFile: String = "path-to-input-file",
    outputFile: String = "path-to-output-file",
    previousIntegrated: String = "path-to-previous-integrated-operators",
    postgressUrl: String = "postgress-url",
    postgressUsername: String = "postgress-username",
    postgressPassword: String = "postgress-password",
    postgressDB: String = "postgress-db"
) extends SparkJobConfig

object OperatorAcmConverter extends SparkJob[DefaultWithDbAndDeltaConfig] with OperatorAcmConverter {

  def transform(
    spark: SparkSession,
    channelMappings: Dataset[ChannelMapping],
    operators: Dataset[Operator],
    previousIntegrated: Dataset[Operator]
  ): Dataset[UFSOperator] = {

    val dailyUfsOperators = createUfsOperators(spark, operators, channelMappings)
    val allPreviousUfsOperators = createUfsOperators(spark, previousIntegrated, channelMappings)

    val newOperators = dailyUfsOperators
      .joinWith(allPreviousUfsOperators,
        dailyUfsOperators("OPR_LNKD_INTEGRATION_ID") === allPreviousUfsOperators("OPR_LNKD_INTEGRATION_ID"),
        JoinType.LeftAnti)
      .filter(_._2 == null)
      .map(_._1)

      val updatedOperators = dailyUfsOperators
      .joinWith(allPreviousUfsOperators,
        dailyUfsOperators("OPR_LNKD_INTEGRATION_ID") === allPreviousUfsOperators("OPR_LNKD_INTEGRATION_ID"),
        JoinType.Inner)
      .map{case (left, right) => {
        (left, left != right)
      }}
      .filter(_._2)
      .map(_._1)

    newOperators.union(updatedOperators)
  }

  override private[spark] def defaultConfig = DefaultWithDbAndDeltaConfig()

  override private[spark] def configParser(): OptionParser[DefaultWithDbAndDeltaConfig] =
    new scopt.OptionParser[DefaultWithDbAndDeltaConfig]("Operator ACM converter") {
      head("converts domain operators into ufs operators.", "1.0")
      opt[String]("inputFile") required () action { (x, c) ⇒
        c.copy(inputFile = x)
      } text "inputFile is a string property"
      opt[String]("previousIntegrated") required () action { (x, c) ⇒
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

  override def run(spark: SparkSession, config: DefaultWithDbAndDeltaConfig, storage: Storage): Unit = {
    import spark.implicits._

    log.info(s"Generating operator ACM csv file from [$config.inputFile] to [$config.outputFile]")

    val channelMappings = storage.channelMappings(config.postgressUrl, config.postgressDB, config.postgressUsername, config.postgressPassword)
    val operators = storage.readFromParquet[Operator](config.inputFile)
    val previousIntegrated = storage.readFromParquet[Operator](config.previousIntegrated)
    val transformed = transform(spark, channelMappings, operators, previousIntegrated)

    writeToCsv(storage, transformed, config.outputFile)
  }
}
