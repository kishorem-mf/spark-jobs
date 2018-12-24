package com.unilever.ohub.spark.merging

import java.util.UUID

import com.unilever.ohub.spark.domain.entity.Campaign
import com.unilever.ohub.spark.sql.JoinType
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.{SparkJob, SparkJobConfig}
import org.apache.spark.sql.{Dataset, SparkSession}
import scopt.OptionParser

case class CampaignClickMergingConfig(
   previousIntegrated: String = "previous-integrated-campaigns",
   campaignClickInputFile: String = "campaign-click-input-file",
   outputFile: String = "path-to-output-file"
) extends SparkJobConfig

/**
  * Simple merging which only passes the latest version of a campaignClick entity
  * (and copies the ohubId when it is previously integrated).
  */
object CampaignClickMerging extends SparkJob[CampaignClickMergingConfig] {

  def transform(
     spark: SparkSession,
     campaigns: Dataset[Campaign],
     previousIntegrated: Dataset[Campaign]
  ): Dataset[Campaign] = {
    import spark.implicits._

    previousIntegrated
      .joinWith(campaigns, previousIntegrated("concatId") === campaigns("concatId"), JoinType.FullOuter)
      .map {
        case (integrated, campaignClick) ⇒
          if (campaignClick == null) {
            integrated
          } else {
            val ohubId = if (integrated == null) Some(UUID.randomUUID().toString) else integrated.ohubId
            campaignClick.copy(ohubId = ohubId, isGoldenRecord = true)
          }
      }
  }

  override private[spark] def defaultConfig = CampaignClickMergingConfig()

  override private[spark] def configParser(): OptionParser[CampaignClickMergingConfig] =
    new scopt.OptionParser[CampaignClickMergingConfig]("Order merging") {
      head("merges campaignsClicks into an integrated campaignClicks output file.", "1.0")
      opt[String]("campaignInputFile") required () action { (x, c) ⇒
        c.copy(campaignClickInputFile = x)
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

  override def run(spark: SparkSession, config: CampaignClickMergingConfig, storage: Storage): Unit = {
    log.info(
      s"Merging campaigns from [${config.campaignClickInputFile}] " +
        s"and [${config.previousIntegrated}] to [${config.outputFile}]"
    )

    val campaignRecords = storage.readFromParquet[Campaign](config.campaignClickInputFile)
    val previousIntegrated = storage.readFromParquet[Campaign](config.previousIntegrated)

    val transformed = transform(spark, campaignRecords, previousIntegrated)

    storage.writeToParquet(transformed, config.outputFile)
  }
}
