package com.unilever.ohub.spark.merging

import java.util.UUID

import com.unilever.ohub.spark.{SparkJob, SparkJobConfig}
import com.unilever.ohub.spark.domain.entity.{Operator, WholesalerAssignment}
import com.unilever.ohub.spark.sql.JoinType
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{Dataset, SparkSession}
import scopt.OptionParser

case class WholesalerAssignmentMergingConfig(
                                       wholesalerAssignmentInputFile: String = "wholesalerassignment-input-file",
                                       previousIntegrated: String = "previous-integrated-file",
                                       operatorIntegrated: String = "operator-integrated-file",
                                       outputFile: String = "path-to-output-file"
                                     ) extends SparkJobConfig

object WholesalerAssignmentMerging extends SparkJob[WholesalerAssignmentMergingConfig] {

  def transform(
                 spark: SparkSession,
                 wholesalerAssignmentInputFile: Dataset[WholesalerAssignment],
                 previousIntegrated: Dataset[WholesalerAssignment],
                 operator: Dataset[Operator]
               ): Dataset[WholesalerAssignment] = {
    import spark.implicits._

    previousIntegrated
      .joinWith(wholesalerAssignmentInputFile, previousIntegrated("concatId") === wholesalerAssignmentInputFile("concatId"), JoinType.FullOuter)
      .map {
        case (integrated, ws) ⇒
          if (ws == null) {
            integrated
          } else {
            val ohubId = if (integrated == null) Some(UUID.randomUUID().toString) else integrated.ohubId
            ws.copy(ohubId = ohubId, isGoldenRecord = true)
          }
      }  // update opr ids
      .joinWith(operator, $"operatorConcatId" === operator("concatId"), "left")
      .map {
        case (ws: WholesalerAssignment, opr: Operator) => ws.copy(operatorOhubId = opr.ohubId)
        case (ws, _) => ws
      }
  }

  override private[spark] def defaultConfig = WholesalerAssignmentMergingConfig()

  override private[spark] def configParser(): OptionParser[WholesalerAssignmentMergingConfig] =
    new scopt.OptionParser[WholesalerAssignmentMergingConfig]("AssetMovement merging") {
      head("merges wholesaler assignment into an integrated wholesaler assignment output file.", "1.0")
      opt[String]("wholesalerassignmentInputFile") required () action { (x, c) ⇒
        c.copy(wholesalerAssignmentInputFile = x)
      } text "WholeSalerAssignmentInputFile is a string property"
      opt[String]("previousIntegrated") required () action { (x, c) ⇒
        c.copy(previousIntegrated = x)
      } text "previousIntegrated is a string property"
      opt[String]("operatorIntegrated") required () action { (x, c) ⇒
        c.copy(operatorIntegrated = x)
      } text "operatorIntegrated is a string property"
      opt[String]("outputFile") required () action { (x, c) ⇒
        c.copy(outputFile = x)
      } text "outputFile is a string property"

      version("1.0")
      help("help") text "help text"
    }

  override def run(spark: SparkSession, config: WholesalerAssignmentMergingConfig, storage: Storage): Unit = {
    log.info(
      s"Merging AssetMovements from [${config.wholesalerAssignmentInputFile}] and  [${config.previousIntegrated}] to [${config.outputFile}]"
    )

    val wholeSalerAssignments = storage.readFromParquet[WholesalerAssignment](config.wholesalerAssignmentInputFile)
    val previousIntegrated = storage.readFromParquet[WholesalerAssignment](config.previousIntegrated)
    val operatorIntegrated = storage.readFromParquet[Operator](config.operatorIntegrated)
    val transformed = transform(spark, wholeSalerAssignments, previousIntegrated,operatorIntegrated)

    storage.writeToParquet(transformed, config.outputFile)
  }
}

