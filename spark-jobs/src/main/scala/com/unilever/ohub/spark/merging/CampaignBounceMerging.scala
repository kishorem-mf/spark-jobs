package com.unilever.ohub.spark.merging

import java.util.UUID

import com.unilever.ohub.spark.domain.entity.CampaignBounce
import com.unilever.ohub.spark.sql.JoinType
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.{SparkJob, SparkJobConfig}
import org.apache.spark.sql.{Dataset, SparkSession}
import scopt.OptionParser

case class CampaignBounceMergingConfig(
   previousIntegrated: String = "previous-integrated-campaigns",
   campaignBounceInputFile: String = "campaign-bounce-input-file",
   outputFile: String = "path-to-output-file"
) extends SparkJobConfig

/**
  * Simple merging which only passes the latest version of a campaignBounce entity
  * (and copies the ohubId when it is previously integrated).
  */
object CampaignBounceMerging extends SparkJob[CampaignBounceMergingConfig] {

  def transform(
     spark: SparkSession,
     campaignBounces: Dataset[CampaignBounce],
     previousIntegrated: Dataset[CampaignBounce]
  ): Dataset[CampaignBounce] = {
    import spark.implicits._

    previousIntegrated
      .joinWith(campaignBounces, previousIntegrated("concatId") === campaignBounces("concatId"), JoinType.FullOuter)
      .map {
        case (integrated, campaignBounce) ⇒
          if (campaignBounce == null) {
            integrated
          } else {
            val ohubId = if (integrated == null) Some(UUID.randomUUID().toString) else integrated.ohubId
            campaignBounce.copy(ohubId = ohubId, isGoldenRecord = true)
          }
      }
  }

  override private[spark] def defaultConfig = CampaignBounceMergingConfig()

  override private[spark] def configParser(): OptionParser[CampaignBounceMergingConfig] =
    new scopt.OptionParser[CampaignBounceMergingConfig]("Order merging") {
      head("merges campaignsBounces into an integrated campaignBounces output file.", "1.0")
      opt[String]("campaignInputFile") required () action { (x, c) ⇒
        c.copy(campaignBounceInputFile = x)
      } text "campaignInputFile is a string property"
      opt[String]("previousIntegrated") optional () action { (x, c) ⇒
        c.copy(previousIntegrated = x)
      } text "previousIntegrated is a string property"
      opt[String]("outputFile") required () action { (x, c) ⇒
        c.copy(outputFile = x)
      } text "outputFile is a string property"

      version("1.0")
      help("help") text "help text"
    }

  override def run(spark: SparkSession, config: CampaignBounceMergingConfig, storage: Storage): Unit = {
    log.info(
      s"Merging campaignBounces from [${config.campaignBounceInputFile}] " +
        s"and [${config.previousIntegrated}] to [${config.outputFile}]"
    )

    val campaignBounceRecords = storage.readFromParquet[CampaignBounce](config.campaignBounceInputFile)
    val previousIntegrated = storage.readFromParquet[CampaignBounce](config.previousIntegrated)

    val transformed = transform(spark, campaignBounceRecords , previousIntegrated)

    storage.writeToParquet(transformed, config.outputFile)
  }
}
