package com.unilever.ohub.spark.merging
import com.unilever.ohub.spark.{SparkJob, SparkJobConfig}
import com.unilever.ohub.spark.domain.entity.Operator
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{Dataset, SparkSession}
import scopt.OptionParser
case class OperatorOldIntegrationIdValidationConfig(
                                           integratedInputFile: String = "path-to-integrated-input-file",
                                           deltaInputFile: String = "path-to-delta-input-file",
                                           outputFile: String = "path-to-valid-output-file"
                                         ) extends SparkJobConfig
object OperatorOldIntegrationIdValidation extends SparkJob[OperatorOldIntegrationIdValidationConfig] {
  override private[spark] def defaultConfig = OperatorOldIntegrationIdValidationConfig()
  override private[spark] def configParser(): OptionParser[OperatorOldIntegrationIdValidationConfig] =
    new scopt.OptionParser[OperatorOldIntegrationIdValidationConfig]("Spark job default") {
      head("run a spark job with default config.", "1.0")
      opt[String]("integratedInputFile") required () action { (x, c) ⇒
        c.copy(integratedInputFile = x)
      } text "integratedInputFile is a string property"
      opt[String]("deltaInputFile") required () action { (x, c) ⇒
        c.copy(deltaInputFile = x)
      } text "deltaInputFile is a string property"
      opt[String]("outputFile") required () action { (x, c) ⇒
        c.copy(outputFile = x)
      } text "outputFile is a string property"
      version("1.0")
      help("help") text "help text"
    }
  def transform(
                 integratedRecords: Dataset[Operator],
                 dailyDeltaRecords: Dataset[Operator])(implicit spark: SparkSession): (Dataset[Operator]) = {
    import spark.implicits._
    val validOhubId = integratedRecords.select($"ohubId" as "validOhubId").distinct()
    val postValidation = dailyDeltaRecords.join(
      validOhubId,
      dailyDeltaRecords("oldIntegrationId") === validOhubId("validOhubId"), "left")
    .drop("oldIntegrationId")
    .select($"*",$"validOhubId" as "oldIntegrationId")
    .drop("validOhubId")
    .as[Operator]
    (postValidation)
  }
  override def run(spark: SparkSession, config: OperatorOldIntegrationIdValidationConfig, storage: Storage): Unit = {
    log.info(
      s"""
         |Ingested operators from  [${config.deltaInputFile}]
         |to output [${config.outputFile}]""".stripMargin)
    implicit val implicitSpark: SparkSession = spark
    val integratedOperators = storage.readFromParquet[Operator](config.integratedInputFile)
    val dailyDeltaOperators = storage.readFromParquet[Operator](config.deltaInputFile)
    val (validDelta) = transform(integratedOperators, dailyDeltaOperators)
    storage.writeToParquet(validDelta, config.outputFile)
  }
}
