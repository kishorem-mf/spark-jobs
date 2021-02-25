package com.unilever.ohub.spark.merging

import java.util.UUID

import com.unilever.ohub.spark.{ SparkJob, SparkJobConfig }
import com.unilever.ohub.spark.domain.entity.AssetMovement
import com.unilever.ohub.spark.sql.JoinType
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{ Dataset, SparkSession }
import scopt.OptionParser

case class AssetMovementMergingConfig(
                                       AssetMovements: String = "answer-input-file",
                                       previousIntegrated: String = "previous-integrated-file",
                                       outputFile: String = "path-to-output-file"
                                     ) extends SparkJobConfig

object AssetMovementMerging extends SparkJob[AssetMovementMergingConfig] {

  def transform(
                 spark: SparkSession,
                 AssetMovements: Dataset[AssetMovement],
                 previousIntegrated: Dataset[AssetMovement]
               ): Dataset[AssetMovement] = {
    import spark.implicits._

    previousIntegrated
      .joinWith(AssetMovements, previousIntegrated("concatId") === AssetMovements("concatId"), JoinType.FullOuter)
      .map {
        case (integrated, answer) ⇒
          if (answer == null) {
            integrated
          } else {
            val ohubId = if (integrated == null) Some(UUID.randomUUID().toString) else integrated.ohubId

            answer.copy(ohubId = ohubId, isGoldenRecord = true)
          }
      }
  }

  override private[spark] def defaultConfig = AssetMovementMergingConfig()

  override private[spark] def configParser(): OptionParser[AssetMovementMergingConfig] =
    new scopt.OptionParser[AssetMovementMergingConfig]("AssetMovement merging") {
      head("merges AssetMovements into an integrated AssetMovements output file.", "1.0")
      opt[String]("AssetMovementsInputFile") required () action { (x, c) ⇒
        c.copy(AssetMovements = x)
      } text "AssetMovementsInputFile is a string property"
      opt[String]("previousIntegrated") required () action { (x, c) ⇒
        c.copy(previousIntegrated = x)
      } text "previousIntegrated is a string property"
      opt[String]("outputFile") required () action { (x, c) ⇒
        c.copy(outputFile = x)
      } text "outputFile is a string property"

      version("1.0")
      help("help") text "help text"
    }

  override def run(spark: SparkSession, config: AssetMovementMergingConfig, storage: Storage): Unit = {
    log.info(
      s"Merging AssetMovements from [${config.AssetMovements}] and  [${config.previousIntegrated}] to [${config.outputFile}]"
    )

    val AssetMovements = storage.readFromParquet[AssetMovement](config.AssetMovements)
    val previousIntegrated = storage.readFromParquet[AssetMovement](config.previousIntegrated)
    val transformed = transform(spark, AssetMovements, previousIntegrated)

    storage.writeToParquet(transformed, config.outputFile)
  }
}
