package com.unilever.ohub.spark.merging

import java.util.UUID

import com.unilever.ohub.spark.domain.entity.Chain
import com.unilever.ohub.spark.sql.JoinType
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.{SparkJob, SparkJobConfig}
import org.apache.spark.sql.{Dataset, SparkSession}
import scopt.OptionParser

case class ChainMergingConfig(
    chains: String = "chains-input-file",
    previousIntegrated: String = "previous-integrated-file",
    outputFile: String = "path-to-output-file"
) extends SparkJobConfig

object ChainMerging extends SparkJob[ChainMergingConfig] {

  def transform(
    spark: SparkSession,
    chains: Dataset[Chain],
    previousIntegrated: Dataset[Chain]
  ): Dataset[Chain] = {
    import spark.implicits._

    previousIntegrated
      .joinWith(chains, previousIntegrated("concatId") === chains("concatId"), JoinType.FullOuter)
      .map {
        case (integrated, chain) ⇒
          if (chain == null) {
            integrated
          } else {
            val ohubId = if (integrated == null) Some(UUID.randomUUID().toString) else integrated.ohubId

            chain.copy(ohubId = ohubId, isGoldenRecord = true)
          }
      }
  }

  override private[spark] def defaultConfig = ChainMergingConfig()

  override private[spark] def configParser(): OptionParser[ChainMergingConfig] =
    new scopt.OptionParser[ChainMergingConfig]("Chain merging") {
      head("merges chains into an integrated chains output file.", "1.0")
      opt[String]("chainsInputFile") required () action { (x, c) ⇒
        c.copy(chains = x)
      } text "chainsInputFile is a string property"
      opt[String]("previousIntegrated") required () action { (x, c) ⇒
        c.copy(previousIntegrated = x)
      } text "previousIntegrated is a string property"
      opt[String]("outputFile") required () action { (x, c) ⇒
        c.copy(outputFile = x)
      } text "outputFile is a string property"

      version("1.0")
      help("help") text "help text"
    }

  override def run(spark: SparkSession, config: ChainMergingConfig, storage: Storage): Unit = {
    log.info(
      s"Merging chains from [${config.chains}] and [${config.previousIntegrated}] to [${config.outputFile}]"
    )

    val chains = storage.readFromParquet[Chain](config.chains)
    val previousIntegrated = storage.readFromParquet[Chain](config.previousIntegrated)
    val transformed = transform(spark, chains, previousIntegrated)

    storage.writeToParquet(transformed, config.outputFile)
  }
}
