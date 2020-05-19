package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.{SparkJob, SparkJobConfig}
import com.unilever.ohub.spark.domain.entity.Operator
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{Dataset, SparkSession}
import scopt.OptionParser

case class StitchIdIngestedWithDbConfig(
                                           integratedInputFile: String = "path-to-integrated-input-file",
                                           deltaInputFile: String = "path-to-delta-input-file",
                                           stitchIdOutputFile: String = "path-to-matched-stitchId-output-file"
                                         ) extends SparkJobConfig

object OperatorStitchId extends SparkJob[StitchIdIngestedWithDbConfig] with GroupingFunctions {

  override private[spark] def defaultConfig = StitchIdIngestedWithDbConfig()

  override private[spark] def configParser(): OptionParser[StitchIdIngestedWithDbConfig] =
    new scopt.OptionParser[StitchIdIngestedWithDbConfig]("Spark job default") {
      head("run a spark job with default config.", "1.0")
      opt[String]("integratedInputFile") required () action { (x, c) ⇒
        c.copy(integratedInputFile = x)
      } text "integratedInputFile is a string property"
      opt[String]("deltaInputFile") required () action { (x, c) ⇒
        c.copy(deltaInputFile = x)
      } text "deltaInputFile is a string property"
      opt[String]("stitchIdOutputFile") required () action { (x, c) ⇒
        c.copy(stitchIdOutputFile = x)
      } text "stitchIdOutputFile is a string property"

      version("1.0")
      help("help") text "help text"
    }

  def transform(
                 integratedRecords: Dataset[Operator],
                 dailyDeltaRecords: Dataset[Operator])(implicit spark: SparkSession): (Dataset[Operator]) = {
    import spark.implicits._
    val fromColumn = $"oldIntegrationId"
    val toColumn = "ohubId"
    val notNullColumns = Seq("oldIntegrationId")
    val stitchOhubIds: Dataset[Operator] = stitchOhubId[Operator](integratedRecords,
      dailyDeltaRecords,toColumn, fromColumn,notNullColumns)

    (stitchOhubIds)
  }

  override def run(spark: SparkSession, config: StitchIdIngestedWithDbConfig, storage: Storage): Unit = {
    log.info(
      s"""
         |Ingested stitched operators from  [${config.deltaInputFile}]
         |to stitch output [${config.stitchIdOutputFile}]""".stripMargin)

    implicit val implicitSpark: SparkSession = spark
    val integratedOperators = storage.readFromParquet[Operator](config.integratedInputFile)
    val dailyDeltaOperators = storage.readFromParquet[Operator](config.deltaInputFile)

    val (matchedStitchIdsDelta) = transform(integratedOperators, dailyDeltaOperators)

    storage.writeToParquet(matchedStitchIdsDelta, config.stitchIdOutputFile)
  }
}
