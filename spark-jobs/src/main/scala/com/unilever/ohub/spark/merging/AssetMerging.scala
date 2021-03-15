package com.unilever.ohub.spark.merging

import java.util.UUID

import com.unilever.ohub.spark.{ SparkJob, SparkJobConfig }
import com.unilever.ohub.spark.domain.entity.Asset
import com.unilever.ohub.spark.sql.JoinType
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{ Dataset, SparkSession }
import scopt.OptionParser

case class AssetMergingConfig(
                                assets: String = "Asset-input-file",
                                previousIntegrated: String = "previous-integrated-file",
                                outputFile: String = "path-to-output-file"
                              ) extends SparkJobConfig

object AssetMerging extends SparkJob[AssetMergingConfig] {

  def transform(
                 spark: SparkSession,
                 assets: Dataset[Asset],
                 previousIntegrated: Dataset[Asset]
               ): Dataset[Asset] = {
    import spark.implicits._

    previousIntegrated
      .joinWith(assets, previousIntegrated("concatId") === assets("concatId"), JoinType.FullOuter)
      .map {
        case (integrated, asset) ⇒
          if (asset == null) {
            integrated
          } else {
            val ohubId = if (integrated == null) Some(UUID.randomUUID().toString) else integrated.ohubId

            asset.copy(ohubId = ohubId, isGoldenRecord = true)
          }
      }
  }

  override private[spark] def defaultConfig = AssetMergingConfig()

  override private[spark] def configParser(): OptionParser[AssetMergingConfig] =
    new scopt.OptionParser[AssetMergingConfig]("Asset merging") {
      head("merges assets into an integrated assets output file.", "1.0")
      opt[String]("assetsInputFile") required () action { (x, c) ⇒
        c.copy(assets = x)
      } text "assetsInputFile is a string property"
      opt[String]("previousIntegrated") required () action { (x, c) ⇒
        c.copy(previousIntegrated = x)
      } text "previousIntegrated is a string property"
      opt[String]("outputFile") required () action { (x, c) ⇒
        c.copy(outputFile = x)
      } text "outputFile is a string property"

      version("1.0")
      help("help") text "help text"
    }

  override def run(spark: SparkSession, config: AssetMergingConfig, storage: Storage): Unit = {
    log.info(
      s"Merging assets from [${config.assets}] and [${config.previousIntegrated}] to [${config.outputFile}]"
    )

    val assets = storage.readFromParquet[Asset](config.assets)
    val previousIntegrated = storage.readFromParquet[Asset](config.previousIntegrated)
    val transformed = transform(spark, assets, previousIntegrated)

    storage.writeToParquet(transformed, config.outputFile)
  }
}
