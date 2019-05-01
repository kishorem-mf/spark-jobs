package com.unilever.ohub.spark.merging

import java.util.UUID

import com.unilever.ohub.spark.domain.entity.ChannelMapping
import com.unilever.ohub.spark.sql.JoinType
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.{SparkJob, SparkJobConfig}
import org.apache.spark.sql.{Dataset, SparkSession}
import scopt.OptionParser

case class ChannelMappingMergingConfig(
    channelMappings: String = "channel-mappings-input-file",
    previousIntegrated: String = "previous-integrated-file",
    outputFile: String = "path-to-output-file"
) extends SparkJobConfig

object ChannelMappingMerging extends SparkJob[ChannelMappingMergingConfig] {

  def transform(
    spark: SparkSession,
    channelMappings: Dataset[ChannelMapping],
    previousIntegrated: Dataset[ChannelMapping]
  ): Dataset[ChannelMapping] = {
    import spark.implicits._

    previousIntegrated
      .joinWith(channelMappings, previousIntegrated("concatId") === channelMappings("concatId"), JoinType.FullOuter)
      .map {
        case (integrated: ChannelMapping, channelMapping: ChannelMapping) => channelMapping.copy(ohubId = integrated.ohubId)
        case (_, channelMapping: ChannelMapping) => channelMapping.copy(ohubId = Some(UUID.randomUUID().toString))
        case (integrated: ChannelMapping, _) => integrated
      }
  }

  override private[spark] def defaultConfig = ChannelMappingMergingConfig()

  override private[spark] def configParser(): OptionParser[ChannelMappingMergingConfig] =
    new scopt.OptionParser[ChannelMappingMergingConfig]("Activity merging") {
      head("merges channelMappings into an integrated channelMappings output file.", "1.0")
      opt[String]("channelMapping") required () action { (x, c) ⇒
        c.copy(channelMappings = x)
      } text "inpuFile is a string property"
      opt[String]("previousIntegrated") required () action { (x, c) ⇒
        c.copy(previousIntegrated = x)
      } text "previousIntegrated is a string property"
      opt[String]("outputFile") required () action { (x, c) ⇒
        c.copy(outputFile = x)
      } text "outputFile is a string property"

      version("1.0")
      help("help") text "help text"
    }

  override def run(spark: SparkSession, config: ChannelMappingMergingConfig, storage: Storage): Unit = {
    log.info(
      s"Merging Channelmappings from [${config.channelMappings}] and [${config.previousIntegrated}] to [${config.outputFile}]"
    )

    val channelMappings = storage.readFromParquet[ChannelMapping](config.channelMappings)
    val previousIntegrated = storage.readFromParquet[ChannelMapping](config.previousIntegrated)
    val transformed = transform(spark, channelMappings, previousIntegrated)

    storage.writeToParquet(transformed, config.outputFile)
  }
}
